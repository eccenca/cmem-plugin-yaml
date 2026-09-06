"""Test different source and target modes"""

import io
import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import yaml
from cmem_client.client import Client
from cmem_client.models.dataset import Dataset, DatasetData
from cmem_client.models.project import Project
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntityPath, EntitySchema
from cmem_plugin_base.dataintegration.parameter.code import YamlCode
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_yaml.parse import SOURCE, TARGET, ParseYaml
from tests import PROJECT_ROOT
from tests.utils import PROJECT_NAME, needs_cmem

DATASET_NAME = "json_dataset"
RESOURCE_NAME = "output_json"
DATASET_TYPE = "json"


@pytest.fixture
def client() -> Client:
    """Provide a Corporate Memory client."""
    return Client.from_env()


@pytest.fixture
def di_environment(client: Client) -> Generator[dict[str, str], Any]:
    """Provide the DI build project incl. assets."""
    client.projects.create_item(Project(name=PROJECT_NAME))
    client.datasets.create_item(
        Dataset(
            id=DATASET_NAME,
            project_id=PROJECT_NAME,
            data=DatasetData(type=DATASET_TYPE, parameters={"file": RESOURCE_NAME}),
        )
    )
    with io.BytesIO(b'{"key": "value"}') as response_file:
        client.datasets.post_file_resource(
            project_id=PROJECT_NAME,
            dataset_id=DATASET_NAME,
            file_resource=response_file,
        )
    yield {"project": PROJECT_NAME, "dataset": DATASET_NAME, "resource": RESOURCE_NAME}
    client.projects.delete_item(PROJECT_NAME)


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

    # source mode 'entities' with an entity which carries no value at all
    schema = EntitySchema(type_uri="urn:x-yaml:document", paths=[EntityPath(path="yaml-src")])
    with pytest.raises(ValueError, match="No value available in entity"):
        ParseYaml(
            source_mode=SOURCE.entities,
            target_mode=TARGET.json_entities,
        ).execute(
            [Entities(iter([Entity(uri="urn:x-yaml:source", values=[])]), schema=schema)],
            TestExecutionContext(),
        )


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
def test_entities_to_json_dataset(client: Client, di_environment: dict) -> None:
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
    plugin.execute(inputs=[entities], context=TestExecutionContext(project_id=PROJECT_NAME))

    resource = client.files.read(f"{di_environment['project']}:{di_environment['resource']}")
    assert resource.decode() == yaml_as_json


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
