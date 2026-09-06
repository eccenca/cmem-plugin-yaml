"""Test different source and target modes"""

import gzip
import json
import zipfile
from pathlib import Path

import pytest
from cmem_plugin_base.dataintegration.context import ExecutionReport, ReportContext
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


class RecordingReport(ReportContext):
    """A report context which keeps what the task reported, so a test can assert on it"""

    def __init__(self) -> None:
        self.reports: list[ExecutionReport] = []

    def update(self, report: ExecutionReport) -> None:
        """Record one report"""
        self.reports.append(report)


def recording_context() -> tuple[TestExecutionContext, RecordingReport]:
    """Build an execution context which records what the task reports"""
    context = TestExecutionContext()
    report = RecordingReport()
    context.report = report
    return context, report


def local_file_entities(*paths: Path) -> Entities:
    """Build an input carrying arbitrary local files as file entities"""
    schema = FileEntitySchema()
    return Entities(iter([schema.to_entity(LocalFile(str(_))) for _ in paths]), schema=schema)


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
def test_reported_counts_are_singular_for_one() -> None:
    """Test that the reported description follows the number it describes"""
    context, report = recording_context()
    ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.file).execute(
        [file_entities("alice.yml")], context
    )
    assert [(_.entity_count, _.operation_desc) for _ in report.reports] == [
        (1, "JSON file returned")
    ]

    context, report = recording_context()
    ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.file).execute(
        [file_entities("alice.yml", "bob.yml")], context
    )
    assert [(_.entity_count, _.operation_desc) for _ in report.reports] == [
        (1, "JSON file returned"),
        (2, "JSON files returned"),
    ]

    context, report = recording_context()
    ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.entities).execute(
        [file_entities("alice.yml")], context
    )
    assert [(_.entity_count, _.operation_desc) for _ in report.reports] == [(1, "document parsed")]
    assert report.reports[-1].summary == [("Document parsed", "1")]


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
        "parsed-yaml.json": {"name": "alice"},
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
    declared_ports = 2
    assert len(plugin.input_ports.ports) == declared_ports
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
    with pytest.raises(ValueError, match="None of the 1 documents could be used"):
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


@needs_cmem
def test_yaml_types_json_does_not_have_are_written_as_text() -> None:
    """Test that a date value does not abort the batch on the way out"""
    result = ParseYaml(
        source_mode=SOURCE.code,
        target_mode=TARGET.file,
        source_code=YamlCode("name: alice\nborn: 1990-01-02"),
    ).execute([], TestExecutionContext())
    assert written_files(result) == {"parsed-yaml.json": {"name": "alice", "born": "1990-01-02"}}


@needs_cmem
def test_a_batch_of_mappings_and_plain_values_is_reported() -> None:
    """Test that mixing a mapping and plain values reports instead of failing inside the builder"""
    with pytest.raises(ValueError, match="Not every document is a mapping"):
        ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.entities).execute(
            [file_entities("alice.yml", "plain-list.yml")], TestExecutionContext()
        )


@needs_cmem
def test_a_gzipped_file_is_read(tmp_path: Path) -> None:
    """Test that a compressed file is decompressed rather than reported as broken YAML"""
    path = tmp_path / "conf.yml.gz"
    with gzip.open(path, "wt", encoding="utf-8") as writer:
        writer.write("name: alice")
    result = ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.file).execute(
        [local_file_entities(path)], TestExecutionContext()
    )
    assert written_files(result) == {"conf.yml.json": {"name": "alice"}}


@needs_cmem
def test_a_skipped_document_is_named_and_explained() -> None:
    """Test that a skipped document names the file it came from, with the reason"""
    context, report = recording_context()
    plugin = ParseYaml(
        source_mode=SOURCE.file,
        target_mode=TARGET.file,
        tolerate_unusable_input=True,
    )
    plugin.execute([file_entities("alice.yml", "broken.yml")], context)
    assert len(plugin.skipped) == 1
    # the input file, not the JSON name it would have been given
    assert "broken.yml" in plugin.skipped[0]
    assert "broken.json" not in plugin.skipped[0]
    assert "could not be parsed" in plugin.skipped[0]
    assert report.reports[-1].warnings == plugin.skipped


@needs_cmem
def test_a_tolerated_empty_batch_still_reports() -> None:
    """Test that an empty result is reported rather than leaving the task blank"""
    for target in (TARGET.file, TARGET.entities):
        context, report = recording_context()
        ParseYaml(
            source_mode=SOURCE.file,
            target_mode=target,
            tolerate_unusable_input=True,
        ).execute([file_entities()], context)
        assert [(_.entity_count, _.operation_desc) for _ in report.reports] == [
            (0, "JSON files returned" if target == TARGET.file else "documents parsed")
        ]


class CancelingWorkflow:
    """A workflow context which reports that the run is being canceled"""

    def status(self) -> str:
        """Report the canceling status"""
        return "Canceling"


@needs_cmem
def test_cancelling_does_not_look_like_a_broken_input() -> None:
    """Test that a canceled run returns quietly instead of blaming the input port"""
    context = TestExecutionContext()
    context.workflow = CancelingWorkflow()  # type: ignore[assignment]
    result = ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.file).execute(
        [file_entities("alice.yml", "bob.yml")], context
    )
    assert written_files(result) == {}


@needs_cmem
def test_documents_disagreeing_about_a_key_keep_their_values() -> None:
    """Test that a string meeting a list is carried as text rather than split up"""
    result = ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.entities).execute(
        [file_entities("args-string.yml", "args-list.yml")], TestExecutionContext()
    )
    values = [_.values for _ in result.entities]
    assert [_.path for _ in result.schema.paths] == ["service", "args"]
    # the string stays one value instead of becoming nine single characters
    assert values == [
        [["web"], ["--verbose"]],
        [["api"], ['["--verbose", "--debug"]']],
    ]


@needs_cmem
def test_archive_entries_are_named_after_the_entry(tmp_path: Path) -> None:
    """Test that entries of one archive do not all collapse onto the archive's name"""
    archive = tmp_path / "config.zip"
    with zipfile.ZipFile(archive, "w") as writer:
        writer.writestr("alice.yml", "name: alice")
        writer.writestr("nested/bob.yml", "name: bob")
    schema = FileEntitySchema()
    entities = Entities(
        iter(
            [
                schema.to_entity(LocalFile(str(archive), entry_path="alice.yml")),
                schema.to_entity(LocalFile(str(archive), entry_path="nested/bob.yml")),
            ]
        ),
        schema=schema,
    )
    result = ParseYaml(source_mode=SOURCE.file, target_mode=TARGET.file).execute(
        [entities], TestExecutionContext()
    )
    assert written_files(result) == {
        "alice.json": {"name": "alice"},
        "bob.json": {"name": "bob"},
    }
