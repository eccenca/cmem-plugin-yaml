"""test parse plugin"""

import json
from pathlib import Path

import pytest
from cmem_plugin_base.dataintegration.description import Plugin

from cmem_plugin_yaml.parse import ParseYaml
from tests import FIXTURE_DIR


def test_constructor_defaults_match_the_parameters() -> None:
    """Test that a task built in Python starts out as the configured one"""
    description = next(_ for _ in Plugin.plugins if _.plugin_class is ParseYaml)
    defaults = {_.name: _.default_value for _ in description.parameters}
    plugin = ParseYaml()
    assert plugin.source_mode == defaults["source_mode"]
    assert plugin.target_mode == defaults["target_mode"]
    assert plugin.source_file == defaults["source_file"]
    assert plugin.target_dataset == defaults["target_dataset"]
    assert plugin.input_schema_type == defaults["input_schema_type"]
    assert plugin.input_schema_path == defaults["input_schema_path"]


def test_success() -> None:
    """Test successful executions"""
    number_of_tasks = 16
    json_file = ParseYaml.yaml2json(Path(f"{FIXTURE_DIR}/test.yml"))
    with Path.open(json_file, encoding="utf-8") as reader:
        parsed_json = dict(json.load(reader))
    assert parsed_json["version"] == "3"
    assert len(parsed_json["tasks"]) == number_of_tasks


def test_fail() -> None:
    """Test failing executions"""
    with pytest.raises(TypeError):
        ParseYaml.yaml2json(Path(f"{FIXTURE_DIR}/will-be-str.yml"))
    with pytest.raises(TypeError):
        ParseYaml.yaml2json(Path(f"{FIXTURE_DIR}/will-be-int.yml"))
