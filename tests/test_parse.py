"""test the parts of the parse plugin which need no deployment"""

import json
from pathlib import Path
from tempfile import mkdtemp

import pytest
import yaml
from cmem_plugin_base.dataintegration.description import Plugin
from cmem_plugin_base.dataintegration.parameter.code import YamlCode
from cmem_plugin_base.dataintegration.ports import FixedSchemaPort, UnknownSchemaPort
from cmem_plugin_base.dataintegration.typed_entities.file import FileEntitySchema

from cmem_plugin_yaml.parse import SOURCE, TARGET, ParseYaml
from tests import FIXTURE_DIR


def test_constructor_defaults_match_the_parameters() -> None:
    """Test that a task built in Python starts out as the configured one"""
    description = next(_ for _ in Plugin.plugins if _.plugin_class is ParseYaml)
    defaults = {_.name: _.default_value for _ in description.parameters}
    plugin = ParseYaml()
    assert plugin.source_mode == defaults["source_mode"]
    assert plugin.target_mode == defaults["target_mode"]
    assert plugin.number_of_inputs == defaults["number_of_inputs"]
    assert plugin.tolerate_unusable_input == defaults["tolerate_unusable_input"]
    assert plugin.input_schema_type == defaults["input_schema_type"]
    assert plugin.input_schema_path == defaults["input_schema_path"]


def test_parse_yaml_success() -> None:
    """Test parsing of well formed documents"""
    number_of_tasks = 16
    with Path.open(Path(FIXTURE_DIR) / "test.yml", "rb") as reader:
        parsed = ParseYaml.parse_yaml(reader)
    assert isinstance(parsed, dict)
    assert parsed["version"] == "3"
    assert len(parsed["tasks"]) == number_of_tasks
    assert ParseYaml.parse_yaml("name: alice") == {"name": "alice"}
    assert ParseYaml.parse_yaml("- 1\n- 2\n") == [1, 2]


def test_parse_yaml_rejects_scalars() -> None:
    """Test that a document which is a bare scalar is refused"""
    for fixture in ("will-be-str.yml", "will-be-int.yml"):
        with Path.open(Path(FIXTURE_DIR) / fixture, "rb") as reader, pytest.raises(TypeError):
            ParseYaml.parse_yaml(reader)


def test_parse_yaml_rejects_broken_and_multi_document() -> None:
    """Test that invalid YAML and a multi document stream are refused"""
    with Path.open(Path(FIXTURE_DIR) / "broken.yml", "rb") as reader, pytest.raises(yaml.YAMLError):
        ParseYaml.parse_yaml(reader)
    with pytest.raises(yaml.YAMLError):
        ParseYaml.parse_yaml("---\nname: alice\n---\nname: bob\n")


def test_write_json() -> None:
    """Test that a parsed document is written as JSON"""
    path = Path(mkdtemp()) / "alice.json"
    assert ParseYaml.write_json({"name": "alice"}, path) == path
    with Path.open(path, encoding="utf-8") as reader:
        assert json.load(reader) == {"name": "alice"}


def test_ports_follow_the_configuration() -> None:
    """Test that the declared ports follow source mode, target mode and the port count"""
    number_of_inputs = 3
    assert ParseYaml(source_mode=SOURCE.code).input_ports.ports == []

    plugin = ParseYaml(source_mode=SOURCE.file, number_of_inputs=number_of_inputs)
    assert len(plugin.input_ports.ports) == number_of_inputs
    for port in plugin.input_ports.ports:
        assert isinstance(port, FixedSchemaPort)
        assert port.schema == FileEntitySchema()

    plugin = ParseYaml(source_mode=SOURCE.entities, input_schema_path="yaml-src")
    assert len(plugin.input_ports.ports) == 1
    assert plugin.input_ports.ports[0].schema.paths[0].path == "yaml-src"

    assert isinstance(ParseYaml(target_mode=TARGET.entities).output_port, UnknownSchemaPort)
    file_output = ParseYaml(target_mode=TARGET.file).output_port
    assert isinstance(file_output, FixedSchemaPort)
    assert file_output.schema == FileEntitySchema()


def test_bad_configuration_is_refused_without_a_deployment() -> None:
    """Test the configuration errors which are raised in the constructor"""
    with pytest.raises(ValueError, match="you need to enter or paste YAML Source Code"):
        ParseYaml(source_mode=SOURCE.code, source_code=YamlCode(""))
    with pytest.raises(ValueError, match="at least one input port"):
        ParseYaml(source_mode=SOURCE.file, number_of_inputs=0)
    with pytest.raises(ValueError, match="Unknown source mode"):
        ParseYaml(source_mode="not-there")
    with pytest.raises(ValueError, match="Unknown target mode"):
        ParseYaml(target_mode="not-there")


def test_keys_are_given_their_string_form() -> None:
    """Test that a YAML key which is not a string becomes one"""
    # `on` is the boolean True under YAML 1.1, which is why a workflow file needs care
    assert ParseYaml.parse_yaml("name: CI\non:\n  push: 1") == {
        "name": "CI",
        "True": {"push": 1},
    }
    assert ParseYaml.parse_yaml("2026-01-01: released") == {"2026-01-01": "released"}
    assert ParseYaml.parse_yaml("80: http") == {"80": "http"}
    # quoting the key in the source keeps it as it reads
    assert ParseYaml.parse_yaml('"on": push') == {"on": "push"}


def test_keys_which_collide_once_stringified_are_refused() -> None:
    """Test that two keys which become the same string are an error, not a lost value"""
    with pytest.raises(ValueError, match="appears twice"):
        ParseYaml.parse_yaml('80: http\n"80": text')
