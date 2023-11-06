"""Load YAML to JSON dataset workflow plugin module"""
import json
from io import BytesIO
from typing import Sequence, Optional

import ruamel.yaml

from cmem.cmempy.api import request
from cmem.cmempy.workspace.projects.datasets.dataset import get_dataset_file_uri

from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    UserContext,
)
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_user_access,
    split_task_id,
)

from cmem_plugin_yaml2json.utils import DatasetParameterType


@Plugin(
    label="YAML to JSON (yaml2json)",
    description="Load a YAML file into a JSON dataset.",
    documentation="""
This example workflow operator generates random values.

The values are generated in X rows a Y values. Both parameter can be specified:

- 'number_of_entities': How many rows do you need.
- 'number_of_values': How many values per row do you need.
""",
    parameters=[
        PluginParameter(
            name="source_file",
            label="Source File",
            description="From which file do you wan to load the data?"
            " The dropdown lists usable files from the current"
            " project only.",
            param_type=DatasetParameterType(dataset_type="json"),
            default_value="",
        ),
        PluginParameter(
            name="target_dataset",
            label="Target Dataset",
            description="Where do you want to save the result of the conversion?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check that it"
            " actually is a JSON dataset.",
            param_type=DatasetParameterType(dataset_type="json"),
            default_value="",
        ),
    ],
)
class Yaml2JsonPlugin(WorkflowPlugin):
    """YAML to JSON Plugin"""

    def __init__(self, source_file: str, target_dataset: str) -> None:
        self.source_file = source_file
        self.target_dataset = target_dataset

    def execute(
        self, inputs: Sequence[Entities], context: ExecutionContext
    ) -> Entities:
        self.log.info("convert")

        self.log.info("Happy to serve yaml2json random values.")

        yaml = ruamel.yaml.YAML(typ='safe')
        with open(self.source_file, encoding="utf8") as source_file_handler:
            data = yaml.load(source_file_handler)

            handler = BytesIO()
            handler.write(json.dumps(data).encode())
            handler.seek(0)

            write_to_dataset(
                dataset_id=self.target_dataset,
                file_resource=handler,
                context=context.user,
            )


def write_to_dataset(
    dataset_id: str, file_resource=None, context: Optional[UserContext] = None
):
    """Write to a dataset.

    Args:
        dataset_id (str): The combined task ID.
        file_resource (file stream): Already opened byte file stream
        context (UserContext):
            The user context to setup environment for accessing CMEM with cmempy.

    Returns:
        requests.Response object

    Raises:
        ValueError: in case the task ID is not splittable
        ValueError: missing parameter
    """
    setup_cmempy_user_access(context=context)
    project_id, task_id = split_task_id(dataset_id)

    return post_resource(
        project_id=project_id,
        dataset_id=task_id,
        file_resource=file_resource,
    )


def post_resource(project_id, dataset_id, file_resource=None):
    """
    Post a resource to a dataset.

    If the dataset resource already exists, posting a new resource will replace it.

    Args:
        project_id (str): The ID of the project.
        dataset_id (str): The ID of the dataset.
        file_resource (io Binary Object, optional): The file resource to be uploaded.

    Returns:
        Response: The response from the request.

    """
    endpoint = get_dataset_file_uri().format(project_id, dataset_id)

    with file_resource as file:
        response = request(
            endpoint,
            method="PUT",
            stream=True,
            data=file,
        )
    return response
