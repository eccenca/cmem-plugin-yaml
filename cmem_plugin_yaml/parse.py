"""Load YAML to JSON dataset workflow plugin module"""
import json
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from tempfile import mkdtemp
from types import SimpleNamespace
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
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.code import YamlCode
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.parameter.resource import ResourceParameterType
from cmem_plugin_base.dataintegration.plugins import PluginLogger, WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FixedSchemaPort,
    UnknownSchemaPort,
)
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from cmem_plugin_base.dataintegration.utils.entity_builder import build_entities_from_data

SOURCE = SimpleNamespace()
SOURCE.entities = "entities"
SOURCE.code = "code"
SOURCE.file = "file"
SOURCE.options = OrderedDict(
    {
        SOURCE.entities: f"{SOURCE.entities}: "
        "Content is parsed from of the input port in a workflow.",
        SOURCE.code: f"{SOURCE.code}: " "Content is parsed from the YAML code field below.",
        SOURCE.file: f"{SOURCE.file}: "
        "Content is parsed from an uploaded project file resource (see advanced options).",
    }
)

TARGET = SimpleNamespace()
TARGET.entities = "entities"
TARGET.json_entities = "json_entities"
TARGET.json_dataset = "json_dataset"
TARGET.options = OrderedDict(
    {
        TARGET.json_entities: f"{TARGET.json_entities}: "
        "Parsed structure will be sent as JSON entities to the output port.",
        TARGET.json_dataset: f"{TARGET.json_dataset}: "
        "Parsed structure will be is saved in a JSON dataset (see advanced options).",
        TARGET.entities: f"{TARGET.entities}: "
        "Parsed structure will be send as entities to the output port.",
    }
)

DEFAULT_YAML = YamlCode(f"# Add your YAML code here (and select '{SOURCE.code}' as input mode).")


@Plugin(
    label="Parse YAML",
    plugin_id="cmem_plugin_yaml-parse",
    description="Parses files, source code or input values as YAML documents.",
    icon=Icon(file_name="logo.svg", package=__package__),
    documentation="""This workflow task is basically a yaml2json command.""",
    parameters=[
        PluginParameter(
            name="source_mode",
            label="Source / Input Mode",
            description="",
            param_type=ChoiceParameterType(SOURCE.options),
            default_value=SOURCE.code,
        ),
        PluginParameter(
            name="target_mode",
            label="Target / Output Mode",
            description="",
            param_type=ChoiceParameterType(TARGET.options),
            default_value=TARGET.entities,
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
        PluginParameter(
            name="input_schema_type",
            label="Input Schema Type / Class",
            description=f"In case of source mode '{SOURCE.entities}', you can specify the "
            "requested input type.",
            advanced=True,
        ),
        PluginParameter(
            name="input_schema_path",
            label="Input Schema Path / Property",
            description=f"In case of source mode '{SOURCE.entities}', you can specify the "
            "requested input path.",
            advanced=True,
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
    input_schema_type: str
    input_schema_path: str

    inputs: Sequence[Entities]
    execution_context: ExecutionContext
    project: str
    temp_dir: str

    def __init__(  # noqa: PLR0913
        self,
        source_mode: str = SOURCE.entities,
        target_mode: str = TARGET.entities,
        source_code: YamlCode = DEFAULT_YAML,
        source_file: str = "",
        target_dataset: str = "",
        input_schema_type: str = "urn:x-eccenca:yaml-document",
        input_schema_path: str = "text",
    ) -> None:
        # pylint: disable=too-many-arguments
        self.source_mode = source_mode
        self.target_mode = target_mode
        self.source_code = str(source_code)
        self.source_file = source_file
        self.target_dataset = target_dataset
        self.input_schema_path = input_schema_path
        self.input_schema_type = input_schema_type
        self._validate_config()
        self._set_ports()

    def _raise_error(self, message: str) -> None:
        """Send a report and raise an error"""
        if hasattr(self, "execution_context"):
            self.execution_context.report.update(
                ExecutionReport(
                    entity_count=0, operation_desc="YAML document parsed", error=message
                )
            )
        raise ValueError(message)

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        match self.source_mode:
            case SOURCE.file:
                # no input port
                self.input_ports = FixedNumberOfInputs([])
            case SOURCE.code:
                # no input port
                self.input_ports = FixedNumberOfInputs([])
            case SOURCE.entities:
                # single input port with fixed minimal schema
                self.input_ports = FixedNumberOfInputs(
                    [
                        FixedSchemaPort(
                            schema=EntitySchema(
                                type_uri=self.input_schema_type,
                                paths=[EntityPath(self.input_schema_path)],
                            )
                        )
                    ]
                )
            case _:
                raise ValueError(f"Unknown source mode: {self.source_mode}")
        match self.target_mode:
            case TARGET.entities:
                # output port with flexible schema
                self.output_port = UnknownSchemaPort()
            case TARGET.json_entities:
                # output port with fixed schema
                self.output_port = FixedSchemaPort(
                    schema=EntitySchema(type_uri="json-document", paths=[EntityPath("text")])
                )
            case TARGET.json_dataset:
                # not output port
                self.output_port = None
            case _:
                raise ValueError(f"Unknown target mode: {self.target_mode}")

    def _validate_config(self) -> None:
        """Raise value errors on bad configurations"""
        if self.source_mode == SOURCE.code and str(self.source_code) == "":
            self._raise_error(
                f"When using the source mode '{SOURCE.code}', "
                "you need to enter or paste YAML Source Code in the code field."
            )
        if self.source_mode == SOURCE.file:
            if self.source_file == "":
                self._raise_error(
                    f"When using the source mode '{SOURCE.file}', "
                    "you need to select a YAML file."
                )
            if hasattr(self, "execution_context") and not resource_exist(
                self.project, self.source_file
            ):
                self._raise_error(f"The file '{self.source_file}' does not exist in the project.")
        if self.target_mode == TARGET.json_dataset and self.target_dataset == "":
            self._raise_error(
                f"When using the target mode '{TARGET.json_dataset}', "
                "you need to select a JSON dataset."
            )

    def _get_input_file(self, writer: BinaryIO) -> None:
        """Get input YAML file from project file."""
        with get_resource_response(self.project, self.source_file) as response:
            for chunk in response.iter_content(chunk_size=8192):
                writer.write(chunk)

    def _get_input_code(self, writer: BinaryIO) -> None:
        """Get input YAML file from direct YAML code"""
        writer.write(self.source_code.encode("utf-8"))

    def _get_input_entities(self, writer: BinaryIO) -> None:
        """Get input YAML from fist path of first entity of first input"""
        try:
            first_input: Entities = self.inputs[0]
        except IndexError as error:
            raise ValueError("Input port not connected.") from error
        try:
            first_entity: Entity = next(first_input.entities)
        except StopIteration as error:
            raise ValueError(
                "No entity available on input port. "
                "Maybe you can re-configure the Input Schema Type / Class in Advanced Options?"
            ) from error
        try:
            first_value: str = next(iter(first_entity.values))[0]
        except IndexError as error:
            raise ValueError(
                "No value available in entity. "
                "Maybe you can re-configure the input Input Schema Path / Property "
                "in Advanced Options?"
            ) from error
        writer.write(first_value.encode("utf-8"))

    def _get_input(self) -> Path:
        """Depending on configuration, gets the YAML from different sources."""
        file_yaml = Path(f"{self.temp_dir}/source.yaml")
        try:
            # Select a _get_input_* function based on source_mode
            get_input = getattr(self, f"_get_input_{self.source_mode}")
        except AttributeError as error:
            raise ValueError(f"Source mode not implemented yet: '{self.source_mode}'") from error
        with Path.open(file_yaml, "wb") as writer:
            get_input(writer)
        return file_yaml

    def _provide_output_json_entities(self, file_json: Path) -> Entities:
        """Output as a single JSON entity"""
        with Path.open(file_json, encoding="utf-8") as reader:
            json_code = reader.read()
        schema = EntitySchema(type_uri="urn:x-json:document", paths=[EntityPath(path="json-src")])
        entities = iter([Entity(uri="urn:x-json:source", values=[[json_code]])])
        self.log.info("JSON provided as single output entity.")
        self.execution_context.report.update(
            ExecutionReport(
                entity_count=1,
                operation_desc="JSON entity generated",
            )
        )
        return Entities(entities=entities, schema=schema)

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
                operation_desc="JSON dataset replaced",
            )
        )

    @staticmethod
    def _provide_output_entities(file_json: Path) -> Entities | None:
        """Output as entities"""
        data = json.loads(Path.open(file_json, encoding="utf-8").read())
        return build_entities_from_data(data=data)

    def _provide_output(self, file_json: Path) -> Entities | None:
        """Depending on configuration, provides the parsed content for different outputs"""
        try:
            # Select a _provide_output_* function based on target_mode
            provide_output = getattr(self, f"_provide_output_{self.target_mode}")
        except AttributeError as error:
            raise ValueError(f"Target mode not implemented yet: '{self.target_mode}'") from error
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
