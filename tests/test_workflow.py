"""Test different source and target modes"""

import io
import json
from pathlib import Path

import pytest
import yaml
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
    get_resource_response,
)
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntityPath, EntitySchema
from cmem_plugin_base.dataintegration.parameter.code import YamlCode

from cmem_plugin_yaml.parse import SOURCE, TARGET, ParseYaml
from tests import PROJECT_ROOT
from tests.utils import PROJECT_NAME, TestExecutionContext, needs_cmem

DATASET_NAME = "json_dataset"
RESOURCE_NAME = "output_json"
DATASET_TYPE = "json"


@pytest.fixture
def di_environment() -> object:
    """Provide the DI build project incl. assets."""
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with io.StringIO('{"key": "value"}') as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )
    yield {"project": PROJECT_NAME, "dataset": DATASET_NAME, "resource": RESOURCE_NAME}
    delete_project(PROJECT_NAME)


@needs_cmem
def test_bad_configurations() -> None:
    """Test some bad configuration"""
    # source mode 'code' without configured YAML code
    with pytest.raises(
        ValueError, match="you need to enter or paste YAML Source Code in the code field"
    ):
        ParseYaml(
            source_mode=SOURCE.code,
            target_mode=TARGET.json_entities,
            source_code=YamlCode(""),
        )

    # source mode 'file' without configured file
    with pytest.raises(ValueError, match="you need to select a YAML file"):
        ParseYaml(
            source_mode=SOURCE.file,
            target_mode=TARGET.json_entities,
        )

    # target mode 'dataset' without configured JSON dataset
    with pytest.raises(ValueError, match="you need to select a JSON dataset"):
        ParseYaml(
            source_mode=SOURCE.code,
            target_mode=TARGET.json_dataset,
            source_code=YamlCode("---"),
        )

    # yaml not complete
    with pytest.raises(TypeError, match="YAML content could not be parsed to a dict or list"):
        ParseYaml(
            source_mode=SOURCE.code,
            target_mode=TARGET.json_entities,
            source_code=YamlCode("---"),
        ).execute([], TestExecutionContext())

    # unknown source mode
    with pytest.raises(ValueError, match="Unknown source mode"):
        ParseYaml(
            source_mode="not-there",
            target_mode=TARGET.json_entities,
            source_code=YamlCode(""),
        ).execute([], TestExecutionContext())

    # unknown target mode
    with pytest.raises(ValueError, match="Unknown target mode"):
        ParseYaml(
            source_mode=SOURCE.code,
            target_mode="not-there",
            source_code=YamlCode("ttt: 123"),
        ).execute([], TestExecutionContext())


@needs_cmem
def test_code_to_json_entities() -> None:
    """Test source to json entities"""
    with Path.open(Path(PROJECT_ROOT) / "Taskfile.yaml") as reader:
        yaml_code = reader.read()
    yaml_as_dict: dict = yaml.safe_load(yaml_code)
    yaml_as_json = json.dumps(yaml_as_dict)

    plugin = ParseYaml(
        source_mode=SOURCE.code,
        target_mode=TARGET.json_entities,
        source_code=YamlCode(yaml_code),
    )
    entities: Entities = plugin.execute([], TestExecutionContext())
    json_result: str = next(entities.entities).values[0][0]
    assert json_result == yaml_as_json


@needs_cmem
def test_entities_to_json_dataset(di_environment: dict) -> None:
    """Test entities to JSON dataset"""
    with Path.open(Path(PROJECT_ROOT) / "Taskfile.yaml") as reader:
        yaml_code = reader.read()
    yaml_as_dict: dict = yaml.safe_load(yaml_code)
    yaml_as_json = json.dumps(yaml_as_dict)

    schema = EntitySchema(type_uri="urn:x-yaml:document", paths=[EntityPath(path="yaml-src")])
    entities = Entities(
        iter([Entity(uri="urn:x-yaml:source", values=[[yaml_code]])]), schema=schema
    )

    plugin = ParseYaml(
        source_mode=SOURCE.entities,
        target_mode=TARGET.json_dataset,
        target_dataset=di_environment["dataset"],
    )
    plugin.execute(inputs=[entities], context=TestExecutionContext())

    with get_resource_response(di_environment["project"], di_environment["resource"]) as response:
        assert response.text == yaml_as_json


@needs_cmem
def test_code_to_entities() -> None:
    """Test source to entities"""
    with Path.open(Path(PROJECT_ROOT) / "Taskfile.yaml") as reader:
        yaml_code = reader.read()
    plugin = ParseYaml(
        source_mode=SOURCE.code,
        target_mode=TARGET.entities,
        source_code=YamlCode(yaml_code),
    )
    schema_and_entities: Entities = plugin.execute([], TestExecutionContext())
    schema = schema_and_entities.schema
    entities = list(schema_and_entities.entities)
    assert schema.paths[0].path == "version"  # first line was "version: "
    assert entities[0].values[0][0] == "3"  # is treated as multi value
