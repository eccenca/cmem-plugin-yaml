"""Test different source and target modes"""

import json
from pathlib import Path

import pytest
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntityPath, EntitySchema
from cmem_plugin_base.dataintegration.parameter.code import YamlCode
from cmem_plugin_base.dataintegration.typed_entities.file import (
    FileEntitySchema,
    LocalFile,
)
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_yaml.parse import SOURCE, TARGET, ParseYaml
from tests import FIXTURE_DIR, PROJECT_ROOT
from tests.utils import needs_cmem

YAML_SCHEMA = EntitySchema(type_uri="urn:x-yaml:document", paths=[EntityPath(path="yaml-src")])


def yaml_entities(*documents: str) -> Entities:
    """Build an input carrying YAML documents as text"""
    entities = [
        Entity(uri=f"urn:x-yaml:source-{number}", values=[[document]])
        for number, document in enumerate(documents, start=1)
    ]
    return Entities(iter(entities), schema=YAML_SCHEMA)


def file_entities(*names: str) -> Entities:
    """Build an input carrying fixture files as file entities"""
    schema = FileEntitySchema()
    entities = [schema.to_entity(LocalFile(str(Path(FIXTURE_DIR) / name))) for name in names]
    return Entities(iter(entities), schema=schema)


def written_files(result: Entities) -> dict[str, dict | list]:
    """Read back what a file output produced, keyed by file name"""
    schema = FileEntitySchema()
    files = [schema.from_entity(entity) for entity in result.entities]
    return {Path(_.path).name: json.loads(Path(_.path).read_text(encoding="utf-8")) for _ in files}


@needs_cmem
def test_bad_configurations() -> None:
    """Test some bad configuration"""
    # yaml is a bare scalar - the TypeError of the seam is reported as a task error
    with pytest.raises(ValueError, match="could not be parsed to a dict or list"):
        ParseYaml(
            source_mode=SOURCE.code,
            source_code=YamlCode("---"),
        ).execute([], TestExecutionContext())

    # an entity which carries no value at all
    with pytest.raises(ValueError, match="carries no value"):
        ParseYaml(source_mode=SOURCE.entities).execute(
            [Entities(iter([Entity(uri="urn:x-yaml:source", values=[])]), schema=YAML_SCHEMA)],
            TestExecutionContext(),
        )

    # nothing on the input port at all
    with pytest.raises(ValueError, match="No entity available on input port"):
        ParseYaml(source_mode=SOURCE.entities).execute([yaml_entities()], TestExecutionContext())
    with pytest.raises(ValueError, match="No file available on input port"):
        ParseYaml(source_mode=SOURCE.file).execute([file_entities()], TestExecutionContext())

    # a list of plain values cannot become entities
    with pytest.raises(ValueError, match="nothing to build entities from"):
        ParseYaml(
            source_mode=SOURCE.file,
            target_mode=TARGET.entities,
        ).execute([file_entities("plain-list.yml")], TestExecutionContext())


@needs_cmem
def test_code_to_entities() -> None:
    """Test the code field to entities"""
    with Path.open(Path(PROJECT_ROOT) / "Taskfile.yaml") as reader:
        yaml_code = reader.read()
    result = ParseYaml(
        source_mode=SOURCE.code,
        target_mode=TARGET.entities,
        source_code=YamlCode(yaml_code),
    ).execute([], TestExecutionContext())
    entities = list(result.entities)
    assert result.schema.paths[0].path == "version"  # first line was "version: "
    assert entities[0].values[0][0] == "3"


@needs_cmem
def test_code_to_file() -> None:
    """Test the code field to a file, which carries the fallback name"""
    result = ParseYaml(
        source_mode=SOURCE.code,
        target_mode=TARGET.file,
        source_code=YamlCode("name: alice"),
    ).execute([], TestExecutionContext())
    assert written_files(result) == {"parsed-yaml.json": {"name": "alice"}}


@needs_cmem
def test_file_to_file_keeps_the_name() -> None:
    """Test that a.yml becomes a.json, one output file per input file"""
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
    ).execute([file_entities("alice.yml", "bob.yml")], TestExecutionContext())
    assert written_files(result) == {
        "alice.json": {"name": "alice", "age": 30},
        "bob.json": {"name": "bob", "city": "berlin"},
    }


@needs_cmem
def test_file_to_entities_unions_the_paths() -> None:
    """Test that several documents become one stream whose paths are unioned"""
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.entities,
    ).execute([file_entities("alice.yml", "bob.yml")], TestExecutionContext())
    entities = list(result.entities)
    assert [_.path for _ in result.schema.paths] == ["name", "age", "city"]
    assert [_.values for _ in entities] == [
        [["alice"], ["30"], [""]],
        [["bob"], [""], ["berlin"]],
    ]


@needs_cmem
def test_entities_source_reads_every_entity() -> None:
    """Test that the YAML-as-text source parses one document per entity"""
    result = ParseYaml(
        source_mode=SOURCE.entities,
        target_mode=TARGET.file,
    ).execute([yaml_entities("name: alice", "name: bob")], TestExecutionContext())
    assert written_files(result) == {
        "parsed-yaml-1.json": {"name": "alice"},
        "parsed-yaml-2.json": {"name": "bob"},
    }


@needs_cmem
def test_several_input_ports_are_read_in_order() -> None:
    """Test that every declared input port is read, in the order it is declared"""
    plugin = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        number_of_inputs=2,
    )
    assert len(plugin.input_ports.ports) == 2  # noqa: PLR2004
    result = plugin.execute(
        [file_entities("alice.yml"), file_entities("bob.yml")], TestExecutionContext()
    )
    assert list(written_files(result)) == ["alice.json", "bob.json"]


@needs_cmem
def test_repeated_file_names_are_made_unique() -> None:
    """Test that the same file name on several ports does not overwrite an earlier result"""
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        number_of_inputs=3,
    ).execute(
        [file_entities("alice.yml"), file_entities("alice.yml"), file_entities("alice.yml")],
        TestExecutionContext(),
    )
    written = written_files(result)
    assert list(written) == ["alice.json", "alice-2.json", "alice-3.json"]
    # every one of them still holds the document it was made from
    assert all(_ == {"name": "alice", "age": 30} for _ in written.values())


@needs_cmem
def test_an_empty_port_beside_a_full_one_is_fine() -> None:
    """Test that one empty port does not stop a batch which has documents"""
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        number_of_inputs=2,
    ).execute([file_entities(), file_entities("alice.yml")], TestExecutionContext())
    assert list(written_files(result)) == ["alice.json"]


@needs_cmem
def test_tolerate_unusable_input() -> None:
    """Test skipping an unparseable document, and tolerating an empty input"""
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        tolerate_unusable_input=True,
    ).execute([file_entities("alice.yml", "broken.yml")], TestExecutionContext())
    assert list(written_files(result)) == ["alice.json"]

    # an input which delivers nothing produces an empty result
    result = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        tolerate_unusable_input=True,
    ).execute([file_entities()], TestExecutionContext())
    assert written_files(result) == {}


@needs_cmem
def test_a_batch_which_fails_completely_is_an_error() -> None:
    """Test that tolerating unusable input still fails when nothing survives"""
    with pytest.raises(ValueError, match="All 1 documents failed to parse"):
        ParseYaml(
            source_mode=SOURCE.file,
            target_mode=TARGET.file,
            tolerate_unusable_input=True,
        ).execute([file_entities("broken.yml")], TestExecutionContext())


@needs_cmem
def test_unknown_modes_are_reported() -> None:
    """Test the dispatch errors of source and target mode"""
    plugin = ParseYaml(source_mode=SOURCE.code, source_code=YamlCode("a: 1"))
    plugin.source_mode = "not-there"
    with pytest.raises(ValueError, match="Source mode not implemented yet"):
        plugin.execute([], TestExecutionContext())
    plugin.source_mode = SOURCE.code
    plugin.target_mode = "not-there"
    with pytest.raises(ValueError, match="Target mode not implemented yet"):
        plugin.execute([], TestExecutionContext())
