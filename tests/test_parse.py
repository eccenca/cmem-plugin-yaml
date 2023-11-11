import json
from pathlib import Path

import pytest

from cmem_plugin_yaml2json.parse import yaml2json
from tests import FIXTURE_DIR


def test_success():
    json_file = yaml2json(Path(f"{FIXTURE_DIR}/test.yml"))
    with open(json_file, "r", encoding="utf-8") as reader:
        parsed_json = json.load(reader)
    assert isinstance(parsed_json, object)
    assert parsed_json["version"] == "3"
    assert len(parsed_json["tasks"]) == 16
    print(json_file)


def test_fail():
    with pytest.raises(ValueError):
        yaml2json(Path(f"{FIXTURE_DIR}/will-be-str.yml"))
    with pytest.raises(ValueError):
        yaml2json(Path(f"{FIXTURE_DIR}/will-be-int.yml"))
