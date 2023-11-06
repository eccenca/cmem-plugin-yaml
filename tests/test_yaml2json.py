"""Plugin tests."""
import io

import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
    get_resource_response,
)

from cmem_plugin_yaml2json.workflow import Yaml2JsonPlugin
from tests.utils import TestExecutionContext, needs_cmem

PROJECT_NAME = "yaml2json_test_project"
DATASET_NAME = "sample_dataset"
RESOURCE_NAME = "sample_dataset.txt"
DATASET_TYPE = "text"


@pytest.fixture
def setup(request):
    """Provides the DI build project incl. assets."""
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with io.StringIO("yaml2json plugin sample file.") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )

    request.addfinalizer(lambda: delete_project(PROJECT_NAME))


@needs_cmem
def test_workflow_execution():
    """Test plugin execution"""
    entities = 100
    values = 10

    plugin = Yaml2JsonPlugin(number_of_entities=entities, number_of_values=values)
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    for item in result.entities:
        assert len(item.values) == len(result.schema.paths)


@needs_cmem
def test_integration_placeholder(setup):
    """Placeholder to write integration testcase with cmem"""
    with get_resource_response(PROJECT_NAME, RESOURCE_NAME) as response:
        assert response.text != ""
