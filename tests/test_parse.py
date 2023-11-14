"""test parse plugin"""
import json
from pathlib import Path

import pytest

from cmem_plugin_yaml.parse import ParseYaml
from tests import FIXTURE_DIR


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
