"""Load YAML to JSON dataset workflow plugin module"""
from pathlib import Path
from tempfile import mkdtemp
from typing import Sequence

from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext
)
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.parameter.resource import ResourceParameterType
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs

from cmem.cmempy.workspace.projects.datasets.dataset import post_resource
from cmem.cmempy.workspace.projects.resources.resource import (
    resource_exist,
    get_resource_response
)


def yaml2json(yaml_file: Path, logger=None) -> Path:
    """Converts a YAML file to a JSON file."""
    json_file = Path(f"{mkdtemp()}/{yaml_file.name}.json")
    with open(json_file, 'w', encoding="utf-8") as writer:
        writer.write('{"ttt": ":-)"}')
        logger.info(f"chunk written to {json_file}")
    return json_file


@Plugin(
    label="Load YAML into a JSON dataset",
    description="Transforms a YAML file or text to JSON "
                "and uploads it to a JSON Dataset.",
    documentation="""This workflow task is basically a yaml2json command.""",
    parameters=[
        PluginParameter(
            name="source_file",
            label="Source File",
            description="Which YAML file do you want to load into a JSON dataset? "
                        "The dropdown shows file resources from the current project.",
            param_type=ResourceParameterType(),
            default_value="",
        ),
        PluginParameter(
            name="target_dataset",
            label="Target Dataset",
            description="Where do you want to save the result of the conversion? "
                        "The dropdown shows JSON datasets from the current project.",
            param_type=DatasetParameterType(dataset_type="json"),
            default_value="",
        ),
    ],
)
class Yaml2JsonPlugin(WorkflowPlugin):
    """YAML to JSON Plugin"""

    def __init__(self, source_file: str, target_dataset: str) -> None:
        if source_file == "":
            raise ValueError(
                "Please select a Source File."
            )
        self.source_file = source_file
        if target_dataset == "":
            raise ValueError(
                "Please select a JSON Dataset."
            )
        self.target_dataset = target_dataset
        # Input and output ports
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None

    def execute(
        self, inputs: Sequence[Entities], context: ExecutionContext
    ) -> None:
        self.log.info("start execute")
        setup_cmempy_user_access(context.user)
        project = context.task.project_id()
        if not resource_exist(project, self.source_file):
            raise ValueError(
                f"Source file '{self.source_file}' does not exist in the project."
            )
        self.log.info(
            f"File resource {self.source_file} exists in project {project}"
        )
        temp_dir = mkdtemp()
        with get_resource_response(project, self.source_file) as response:
            file_yaml = Path(f"{temp_dir}/{self.source_file}")
            with open(file_yaml, 'wb') as writer:
                for chunk in response.iter_content(chunk_size=8192):
                    writer.write(chunk)
        self.log.info(
            f"YAML downloaded to {file_yaml}"
        )
        file_json = yaml2json(file_yaml, logger=self.log)
        self.log.info(
            f"JSON created in {file_json}"
        )
        with open(file_json, "r", encoding="utf-8") as reader:
            post_resource(
                project_id=project,
                dataset_id=self.target_dataset,
                file_resource=reader,
            )
        self.log.info(
            f"JSON uploaded to dataset '{self.target_dataset}'."
        )
