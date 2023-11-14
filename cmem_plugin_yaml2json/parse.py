"""Load YAML to JSON dataset workflow plugin module"""
import json
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from tempfile import mkdtemp
from typing import BinaryIO

import yaml
from cmem.cmempy.workspace.projects.datasets.dataset import post_resource
from cmem.cmempy.workspace.projects.resources.resource import (
    get_resource_response,
    resource_exist,
)
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
)
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.code import YamlCode
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.parameter.resource import ResourceParameterType
from cmem_plugin_base.dataintegration.plugins import PluginLogger, WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FlexibleSchemaPort,
)
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access

SOURCE_INPUT = "input"
SOURCE_CODE = "code"
SOURCE_FILE = "file"
SOURCE_OPTIONS = OrderedDict(
    {
        SOURCE_INPUT: f"{SOURCE_INPUT}: "
        "Content is parsed from an input port in a workflow (default).",
        SOURCE_CODE: f"{SOURCE_CODE}: " "Content is parsed from the YAML code field below.",
        SOURCE_FILE: f"{SOURCE_FILE}: "
        "Content is parsed from an uploaded project file resource (advanced option).",
    }
)

TARGET_OUTPUT = "output"
TARGET_JSON_OUTPUT = "json_output"
TARGET_JSON_DATASET = "json_dataset"
TARGET_OPTIONS = OrderedDict(
    {
        TARGET_JSON_OUTPUT: f"{TARGET_JSON_OUTPUT}: "
        "Parsed structure will be send as JSON to the output port (current default).",
        TARGET_JSON_DATASET: f"{TARGET_JSON_DATASET}: "
        "Parsed structure will be is saved in a JSON dataset (advanced option).",
        TARGET_OUTPUT: f"{TARGET_OUTPUT}: "
        "Parsed structure will be send as entities to the output port "
        "(not implemented yet, later default).",
    }
)

DEFAULT_YAML = YamlCode("# enter your YAML code here")


@Plugin(
    label="Parse YAML",
    plugin_id="cmem_plugin_yaml-parse",
    description="Parses files, source code or input values as YAML documents.",
    icon=Icon(file_name="logo.svg", package=__package__),
    documentation="""This workflow task is basically a yaml2json command.""",
    parameters=[
        PluginParameter(
            name="source_mode",
            label="Source",
            description="",
            param_type=ChoiceParameterType(SOURCE_OPTIONS),
        ),
        PluginParameter(
            name="target_mode",
            label="Target",
            description="",
            param_type=ChoiceParameterType(TARGET_OPTIONS),
            default_value=TARGET_JSON_OUTPUT,
        ),
        PluginParameter(
            name="source_code",
            label="YAML Source Code (when using the *code* input)",
        ),
        PluginParameter(
            name="source_file",
            label="YAML File (when using the *file* input)",
            description="Which YAML file do you want to load into a JSON dataset? "
            "The dropdown shows file resources from the current project.",
            param_type=ResourceParameterType(),
            advanced=True,
            default_value="",
        ),
        PluginParameter(
            name="target_dataset",
            label="Target Dataset",
            description="Where do you want to save the result of the conversion? "
            "The dropdown shows JSON datasets from the current project.",
            param_type=DatasetParameterType(dataset_type="json"),
            advanced=True,
            default_value="",
        ),
    ],
)
class ParseYaml(WorkflowPlugin):
    """Parses files, source code or input values as YAML documents."""

    # pylint: disable=too-many-instance-attributes

    source_mode: str
    target_mode: str
    source_code: str
    source_file: str
    target_dataset: str

    inputs: Sequence[Entities]
    execution_context: ExecutionContext
    project: str
    temp_dir: str

    def __init__(  # noqa: PLR0913
        self,
        source_mode: str = SOURCE_INPUT,
        target_mode: str = TARGET_OUTPUT,
        source_code: YamlCode = DEFAULT_YAML,
        source_file: str = "",
        target_dataset: str = "",
    ) -> None:
        # pylint: disable=too-many-arguments
        self.source_mode = source_mode
        self.target_mode = target_mode
        self.source_code = str(source_code)
        self.source_file = source_file
        self.target_dataset = target_dataset
        self._validate_config()
        self._set_ports()

    def _raise_error(self, message: str) -> None:
        """Send a report and raise an error"""
        if self.execution_context:
            self.execution_context.report.update(
                ExecutionReport(
                    entity_count=0, operation_desc="YAML document parsed", error=message
                )
            )
        raise ValueError(message)

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None

        if self.source_mode == SOURCE_INPUT:
            self.input_ports = FixedNumberOfInputs([FlexibleSchemaPort()])
        if self.target_mode == TARGET_OUTPUT:
            self.output_port = FlexibleSchemaPort()

    def _validate_config(self) -> None:
        """Raise value errors on bad configurations"""
        if self.source_mode == SOURCE_CODE and str(self.source_code) == "":
            self._raise_error(
                f"When using the source mode '{SOURCE_CODE}', "
                "you need to enter or paste YAML Source Code in the code field."
            )
        if self.source_mode == SOURCE_FILE:
            if self.source_file == "":
                self._raise_error(
                    f"When using the source mode '{SOURCE_FILE}', "
                    "you need to select a YAML file."
                )
            if self.execution_context and not resource_exist(self.project, self.source_file):
                self._raise_error(
                    f"The file '{self.source_file}' does not exist " "in the project."
                )
        if self.target_mode == TARGET_JSON_DATASET and self.target_dataset == "":
            self._raise_error(
                f"When using the target mode '{TARGET_JSON_DATASET}', "
                "you need to select a JSON dataset."
            )

    def _get_input_file(self, writer: BinaryIO) -> None:
        """Get input YAML file from project file."""
        with get_resource_response(self.project, self.source_file) as response:
            for chunk in response.iter_content(chunk_size=8192):
                writer.write(chunk)

    def _get_input_code(self, writer: BinaryIO) -> None:
        """Get input YAML file from direct YAML code."""
        writer.write(self.source_code.encode("utf-8"))

    def _get_input_input(self, writer: BinaryIO) -> None:
        """Get input YAML file from fist path of first entity of first input."""
        first_input: Entities = self.inputs[0]
        first_entity: Entity = next(first_input.entities)
        first_value: str = next(iter(first_entity.values))[0]
        writer.write(first_value.encode("utf-8"))

    def _get_input(self) -> Path:
        """Depending on configuration, gets the YAML from different sources."""
        file_yaml = Path(f"{self.temp_dir}/source.yaml")
        try:
            # Select a _get_input_* function based on source_mode
            get_input = getattr(self, f"_get_input_{self.source_mode}")
        except AttributeError as error:
            raise ValueError(f"Source mode {self.source_mode} not implemented yet.") from error
        with Path.open(file_yaml, "wb") as writer:
            get_input(writer)
        return file_yaml

    def _provide_output_json_dataset(self, file_json: Path) -> None:
        """Output as JSON to a dataset resource file"""
        with Path.open(file_json, encoding="utf-8") as reader:
            post_resource(
                project_id=self.project,
                dataset_id=self.target_dataset,
                file_resource=reader,
            )
        self.log.info(f"JSON uploaded to dataset '{self.target_dataset}'.")
        self.execution_context.report.update(
            ExecutionReport(
                entity_count=1,
                operation_desc="YAML document parsed",
            )
        )

    def _provide_output(self, file_json: Path) -> Entities | None:
        """Depending on configuration, provides the parsed content for different outputs"""
        try:
            # Select a _provide_output_* function based on target_mode
            provide_output = getattr(self, f"_provide_output_{self.target_mode}")
        except AttributeError as error:
            raise ValueError(f"Target mode {self.target_mode} not implemented yet.") from error
        return provide_output(file_json)

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities | None:
        """Execute the workflow plugin on a given sequence of entities"""
        self.log.info("start execution")
        self.inputs = inputs
        self.execution_context = context
        self.project = self.execution_context.task.project_id()
        self._validate_config()
        setup_cmempy_user_access(context.user)
        self.temp_dir = mkdtemp()
        file_yaml = self._get_input()
        file_json = self.yaml2json(file_yaml, logger=self.log)
        return self._provide_output(file_json)

    @staticmethod
    def yaml2json(yaml_file: Path, logger: PluginLogger = None) -> Path:
        """Convert a YAML file to a JSON file."""
        json_file = Path(f"{mkdtemp()}/{yaml_file.name}.json")
        with Path.open(yaml_file, encoding="utf-8") as yaml_reader:
            yaml_content = yaml.safe_load(yaml_reader)
        if not isinstance(yaml_content, dict | list):
            raise TypeError("YAML content could not be parsed to a dict or list.")
        with Path.open(json_file, "w", encoding="utf-8") as json_writer:
            json.dump(yaml_content, json_writer)
        if logger:
            logger.info(f"JSON written to {json_file}")
        return json_file
