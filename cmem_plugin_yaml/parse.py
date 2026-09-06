"""Parse YAML documents into JSON workflow plugin module"""

import json
import zipfile
from collections import OrderedDict
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from types import SimpleNamespace
from typing import IO, NoReturn

import yaml
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
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FixedSchemaPort,
    Port,
    UnknownSchemaPort,
)
from cmem_plugin_base.dataintegration.typed_entities.file import (
    File,
    FileEntitySchema,
    LocalFile,
)
from cmem_plugin_base.dataintegration.utils.entity_builder import build_entities_from_data

SOURCE = SimpleNamespace()
SOURCE.code = "code"
SOURCE.entities = "entities"
SOURCE.file = "file"
SOURCE.options = OrderedDict(
    {
        SOURCE.code: f"{SOURCE.code} - the YAML is typed or pasted into the code field "
        "of this task",
        SOURCE.entities: f"{SOURCE.entities} - the YAML arrives on an input port as text, "
        "one document per entity",
        SOURCE.file: f"{SOURCE.file} - files arrive on an input port, one document per file",
    }
)

TARGET = SimpleNamespace()
TARGET.entities = "entities"
TARGET.file = "file"
TARGET.options = OrderedDict(
    {
        TARGET.entities: f"{TARGET.entities} - the structure of the documents, as entities",
        TARGET.file: f"{TARGET.file} - one JSON file per document, on the output port",
    }
)

DEFAULT_YAML = YamlCode(f"# Add your YAML code here (and select '{SOURCE.code}' as input mode).")

# The name a written file falls back to when the document it holds came from no file of
# its own - the code field, or an entity carrying YAML as text.
FALLBACK_NAME = "parsed-yaml"

# Target modes which existed until 2.0.0. A task configured before the upgrade still names
# one of them, and "Unknown target mode" reads like a typo rather than a removal.
REMOVED_TARGETS = {
    "json_dataset": f"It was removed in 2.0.0: use the '{TARGET.file}' target mode and a "
    "task which stores project resources.",
    "json_entities": f"It was removed in 2.0.0: use the '{TARGET.file}' target mode, which "
    "returns the JSON document as a file.",
}


@dataclass
class Document:
    """A parsed YAML document, and the name a JSON file made from it would carry.

    :param data: the parsed document, always a mapping or a sequence
    :param name: the file name to write it under, or None when the document came from
        the code field or from an entity value rather than from a file
    """

    data: dict | list
    name: str | None = None


@Plugin(
    label="Parse YAML",
    plugin_id="cmem_plugin_yaml-parse",
    description="Parses YAML documents from a code field, entities or files "
    "and converts them to JSON.",
    icon=Icon(file_name="logo.svg", package=__package__),
    documentation="""
This task reads YAML documents, converts each of them to JSON, and hands the result on
either as entities or as files.

The YAML comes from one of three places: the code field of this task, entities carrying it
as text, or files arriving on an input port. Both port modes read every entity of every
port the task declares, so a batch of files becomes a batch of results, and the number of
those ports is configurable, for documents which come from several tasks at once.
Configured to read from its code field, it declares no input port and starts the
workflow. On the output
side the task either hands on the structure of the documents as entities, or one JSON file
per document.

It usually sits between a task that produces files and one that consumes them: read a
directory of YAML, parse it here, and let a task that stores project resources keep the
JSON. Feeding the entities output into a transformation is the other common chain.

Worth knowing before configuring it:

- A document has to describe a mapping or a sequence. A file holding nothing but a string
  or a number is rejected, since neither becomes a JSON object.
- Exactly one document is read per file. A stream of several documents separated by `---`
  fails.
- Parsing is safe, so YAML tags that construct arbitrary Python objects are refused.
- Documents leaving as entities are combined into one stream whose paths are the union of
  all of them, and a value a document does not carry becomes empty. Where two documents
  disagree about a key - a string in one, a list in the next - the key is carried as text
  in both, since one schema cannot hold it both ways.
- A key which is not a string becomes one. Mind that `on`, `yes` and `no` are booleans in
  YAML, so `on:` reads back as `True` unless it is quoted in the source document.
- A returned file is named after the file it came from - after the entry, when that file
  is an entry in an archive - with a `.json` suffix, made unique when two of them would
  otherwise share a name.
""",
    parameters=[
        PluginParameter(
            name="source_mode",
            label="Source / Input Mode",
            description="Where the YAML documents come from.",
            param_type=ChoiceParameterType(SOURCE.options),
            default_value=SOURCE.code,
        ),
        PluginParameter(
            name="target_mode",
            label="Target / Output Mode",
            description="In which shape the parsed documents leave this task.",
            param_type=ChoiceParameterType(TARGET.options),
            default_value=TARGET.entities,
        ),
        PluginParameter(
            name="source_code",
            label="YAML Source Code (when using the *code* input)",
            description=f"The YAML document itself, used in the '{SOURCE.code}' source mode.",
        ),
        PluginParameter(
            name="number_of_inputs",
            label="Number of Input Ports",
            description="How many input ports the task declares. Raise it to read from "
            "several tasks at once, in the order the ports are shown. It has no effect in "
            f"the '{SOURCE.code}' source mode, which declares no input port at all.",
            advanced=True,
            default_value=1,
        ),
        PluginParameter(
            name="tolerate_unusable_input",
            label="Tolerate Unusable Input",
            description="If enabled, a document that cannot be parsed is reported and "
            "skipped instead of stopping the task, and an input that delivers nothing at "
            "all produces an empty result. A batch in which every document fails remains "
            "an error, since something arrived and none of it could be used.",
            advanced=True,
            default_value=False,
        ),
        PluginParameter(
            name="input_schema_type",
            label="Input Schema Type / Class",
            description="The type the input ports ask for, used in the "
            f"'{SOURCE.entities}' source mode. Change it when the preceding task delivers "
            "entities of a different type.",
            advanced=True,
        ),
        PluginParameter(
            name="input_schema_path",
            label="Input Schema Path / Property",
            description="The path the YAML document is read from, used in the "
            f"'{SOURCE.entities}' source mode. Change it when the preceding task carries "
            "the document in a differently named path.",
            advanced=True,
        ),
    ],
)
class ParseYaml(WorkflowPlugin):
    """Parses YAML documents from a code field, entities or files and converts them to JSON."""

    source_mode: str
    target_mode: str
    source_code: str
    number_of_inputs: int
    tolerate_unusable_input: bool
    input_schema_type: str
    input_schema_path: str

    execution_context: ExecutionContext
    skipped: list[str]
    reported: tuple[int, str]
    canceled: bool

    def __init__(  # noqa: PLR0913 PLR0917
        self,
        # The defaults repeat the default_value of the matching PluginParameter, so that a
        # task built in Python starts out as the one the workflow editor creates.
        source_mode: str = SOURCE.code,
        target_mode: str = TARGET.entities,
        source_code: YamlCode = DEFAULT_YAML,
        number_of_inputs: int = 1,
        tolerate_unusable_input: bool = False,
        input_schema_type: str = "urn:x-eccenca:yaml-document",
        input_schema_path: str = "text",
    ) -> None:
        self.source_mode = source_mode
        self.target_mode = target_mode
        self.source_code = str(source_code)
        self.number_of_inputs = number_of_inputs
        self.tolerate_unusable_input = tolerate_unusable_input
        self.input_schema_path = input_schema_path
        self.input_schema_type = input_schema_type
        self.skipped = []
        self.reported = (0, "documents parsed")
        self.canceled = False
        self._validate_config()
        self._set_ports()

    def _raise_error(self, message: str) -> NoReturn:
        """Send a report and raise an error"""
        if hasattr(self, "execution_context"):
            # keep the terms of whatever the run last reported, so the failure does not
            # reset a count which was reached, nor rename the step which reached it
            count, description = self.reported
            self.execution_context.report.update(
                ExecutionReport(
                    entity_count=count,
                    operation_desc=description,
                    warnings=self.skipped,
                    error=message,
                )
            )
        raise ValueError(message)

    def _validate_config(self) -> None:
        """Raise value errors on bad configurations"""
        if self.source_mode == SOURCE.code and self.source_code == "":
            self._raise_error(
                f"When using the source mode '{SOURCE.code}', "
                "you need to enter or paste YAML Source Code in the code field."
            )
        if self.number_of_inputs < 1:
            self._raise_error("The task needs at least one input port.")

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        match self.source_mode:
            case SOURCE.code:
                # the YAML is configured on the task itself, so nothing is read from a port
                self.input_ports = FixedNumberOfInputs([])
            case SOURCE.entities:
                self.input_ports = FixedNumberOfInputs(
                    [self._entities_port() for _ in range(self.number_of_inputs)]
                )
            case SOURCE.file:
                self.input_ports = FixedNumberOfInputs(
                    [
                        FixedSchemaPort(schema=FileEntitySchema())
                        for _ in range(self.number_of_inputs)
                    ]
                )
            case _:
                self._raise_error(f"Unknown source mode: '{self.source_mode}'.")
        match self.target_mode:
            case TARGET.entities:
                # the schema follows the documents, so it is not known before they are read
                self.output_port = UnknownSchemaPort()
            case TARGET.file:
                self.output_port = FixedSchemaPort(schema=FileEntitySchema())
            case _:
                removed = REMOVED_TARGETS.get(self.target_mode, "")
                self._raise_error(f"Unknown target mode: '{self.target_mode}'. {removed}".strip())

    def _entities_port(self) -> Port:
        """Build an input port for the YAML-as-text source mode"""
        return FixedSchemaPort(
            schema=EntitySchema(
                type_uri=self.input_schema_type,
                paths=[EntityPath(self.input_schema_path)],
            )
        )

    def _canceled(self) -> bool:
        """Check whether the workflow is being canceled, and remember that it was.

        The answer is remembered because a canceled read is indistinguishable from an
        empty one by its result: both return no documents, and only this flag keeps the
        task from telling a user who pressed Cancel that their input port is misconfigured.
        """
        with suppress(AttributeError):
            # context.workflow is absent in some contexts, notably the test contexts
            if self.execution_context.workflow.status() == "Canceling":
                self.canceled = True
        return self.canceled

    def _skip_or_raise(self, message: str) -> None:
        """Skip an unusable document, or stop the task over it.

        The whole message is what is kept, not just the name of the thing: it ends up in
        ExecutionReport.warnings, where a bare list of file names would not tell a workflow
        author whether the cause was bad YAML, an unreadable file or a bare scalar.
        """
        if not self.tolerate_unusable_input:
            self._raise_error(message)
        self.log.warning(f"{message} - skipped.")
        self.skipped.append(message)

    def _parse_document(
        self, source: IO[bytes] | str, label: str, name: str | None
    ) -> Document | None:
        """Parse one document, skipping it when it is unusable and that is tolerated.

        The label names the document as it arrived - the file it was read from, or the
        entity it came out of - while the name is what a written file would be called.
        They are not the same string, and reporting the second one names a file which
        does not exist.
        """
        try:
            return Document(data=self.parse_yaml(source), name=name)
        except (yaml.YAMLError, TypeError, ValueError) as error:
            self._skip_or_raise(f"{label} could not be parsed: {error}")
            return None

    def _port_entities(self, inputs: Sequence[Entities], index: int) -> list[Entity]:
        """Collect the entities of one declared port, warning when it delivered none"""
        entities = list(inputs[index].entities) if index < len(inputs) else []
        if not entities:
            self.log.warning(f"Input port {index + 1} delivered no entities.")
        return entities

    def _warn_about_undeclared_inputs(self, inputs: Sequence[Entities]) -> None:
        """Warn about inputs beyond the declared ports, which are never read.

        The workflow editor only offers the handlers a task declares, so this cannot be
        reached by configuring a workflow - it guards an invariant DataIntegration owns
        rather than this task, and says so rather than dropping a whole port in silence.
        """
        if len(inputs) > self.number_of_inputs:
            self.log.warning(
                f"{len(inputs)} inputs were delivered while {self.number_of_inputs} input "
                "port(s) are declared. Only the declared ones are read."
            )

    def _get_input_code(self, _inputs: Sequence[Entities]) -> list[Document]:
        """Get the document from the YAML code field"""
        document = self._parse_document(self.source_code, "The YAML source code", None)
        return [document] if document else []

    def _get_input_entities(self, inputs: Sequence[Entities]) -> list[Document]:
        """Get one document per entity, read from the configured input path"""
        documents: list[Document] = []
        self._warn_about_undeclared_inputs(inputs)
        for index in range(self.number_of_inputs):
            for number, entity in enumerate(self._port_entities(inputs, index), start=1):
                if self._canceled():
                    return documents
                label = f"Entity {number} of input port {index + 1}"
                try:
                    text: str = next(iter(entity.values))[0]
                except (StopIteration, IndexError):
                    self._skip_or_raise(
                        f"{label} carries no value. Maybe you can re-configure the Input "
                        "Schema Path / Property in Advanced Options?"
                    )
                    continue
                if document := self._parse_document(text, label, None):
                    documents.append(document)
        return documents

    def _get_input_file(self, inputs: Sequence[Entities]) -> list[Document]:
        """Get one document per file arriving on an input port"""
        documents: list[Document] = []
        schema = FileEntitySchema()
        self._warn_about_undeclared_inputs(inputs)
        for index in range(self.number_of_inputs):
            for entity in self._port_entities(inputs, index):
                if self._canceled():
                    return documents
                try:
                    file: File = schema.from_entity(entity)
                    # entry_path first: several entries of one archive share its path, and
                    # naming them all after it collapses them into one name
                    name = Path(file.entry_path or file.path).with_suffix(".json").name
                except (ValueError, IndexError) as error:
                    self._skip_or_raise(
                        f"A file entity on input port {index + 1} is malformed: {error}"
                    )
                    continue
                try:
                    # text_stream rather than read_stream, because it decompresses a
                    # gzipped file on the way; the context is what keeps the read on
                    # cmem-client rather than the deprecated cmempy
                    with file.text_stream(context=self.execution_context) as stream:
                        content = stream.read()
                except (OSError, ValueError, zipfile.BadZipFile) as error:
                    self._skip_or_raise(f"{file.path} could not be read: {error}")
                    continue
                if document := self._parse_document(content, file.path, name):
                    documents.append(document)
        return documents

    def _get_input(self, inputs: Sequence[Entities]) -> list[Document]:
        """Depending on configuration, get the documents from different sources"""
        try:
            # Select a _get_input_* function based on source_mode
            get_input = getattr(self, f"_get_input_{self.source_mode}")
        except AttributeError as error:
            raise ValueError(f"Source mode not implemented yet: '{self.source_mode}'") from error
        documents: list[Document] = get_input(inputs)
        if self.canceled:
            self.log.info("Canceled - returning what had been read.")
            return documents
        if not documents and self.skipped:
            shown = "; ".join(self.skipped[:3])
            more = len(self.skipped) - 3
            self._raise_error(
                f"None of the {len(self.skipped)} documents could be used: {shown}"
                + (f"; and {more} more." if more > 0 else ".")
            )
        if not documents and not self.tolerate_unusable_input:
            self._raise_error(self._nothing_arrived_message())
        return documents

    def _nothing_arrived_message(self) -> str:
        """Explain an empty batch in the terms of the configured source mode"""
        if self.source_mode == SOURCE.file:
            return "No file available on input port. Check the task connected to it."
        return (
            "No entity available on input port. "
            "Maybe you can re-configure the Input Schema Type / Class in Advanced Options?"
        )

    def _report(self, count: int, operation: str, singular: str, plural: str) -> None:
        """Report how much has been done so far, counting the thing by its own name"""
        description = singular if count == 1 else plural
        self.reported = (count, description)
        summary = [(description[0].upper() + description[1:], str(count))]
        if self.skipped:
            summary.append(("Documents skipped", str(len(self.skipped))))
        self.execution_context.report.update(
            ExecutionReport(
                entity_count=count,
                operation=operation,
                operation_desc=description,
                warnings=self.skipped,
                summary=summary,
            )
        )

    def _provide_output_entities(self, documents: list[Document]) -> Entities:
        """Output the structure of the documents as entities"""
        if not documents:
            self._report(0, "read", "document parsed", "documents parsed")
            return Entities(entities=iter([]), schema=EntitySchema(type_uri="", paths=[]))
        data: dict | list = (
            documents[0].data
            if len(documents) == 1
            else [
                item
                for document in documents
                for item in (document.data if isinstance(document.data, list) else [document.data])
            ]
        )
        # build_entities_from_data reads every item of a list as a mapping. It returns
        # None when none of them is one, but raises deep inside itself when only some are,
        # which batching several documents made reachable - so check before calling it.
        if isinstance(data, list) and not all(isinstance(_, dict) for _ in data):
            self._raise_error(
                "Not every document is a mapping, so there is nothing to build entities "
                "from. A YAML list of plain values cannot become entities - use the "
                f"'{TARGET.file}' target mode for it."
            )
        if isinstance(data, list):
            data = self._coerce_conflicting_values(data)
        entities = build_entities_from_data(data)
        if entities is None:
            self._raise_error(
                "The documents hold no mappings, so there is nothing to build entities "
                "from. A YAML list of plain values cannot become entities - use the "
                f"'{TARGET.file}' target mode for it."
            )
        self._report(len(documents), "read", "document parsed", "documents parsed")
        return entities

    def _unique_name(self, name: str, taken: set[str]) -> str:
        """Make a file name unique among the ones already returned by this run.

        Two input ports can deliver files of the same name, and a task which stores what
        this one returns names the resource after the file's basename - so a repeated name
        would overwrite an earlier result rather than collide visibly.
        """
        if name not in taken:
            taken.add(name)
            return name
        stem, suffix = Path(name).stem, Path(name).suffix
        number = 2
        while f"{stem}-{number}{suffix}" in taken:
            number += 1
        unique = f"{stem}-{number}{suffix}"
        self.log.warning(f"'{name}' was returned before, so this document is named '{unique}'.")
        taken.add(unique)
        return unique

    def _provide_output_file(self, documents: list[Document]) -> Entities:
        """Output one JSON file per document"""
        schema = FileEntitySchema()
        files: list[File] = []
        taken: set[str] = set()
        if not documents:
            self._report(0, "write", "JSON file returned", "JSON files returned")
            return Entities(entities=iter([]), schema=schema)
        # one directory for the whole run: _unique_name already guarantees the names in it
        # are distinct, and a directory per document would leak one per file
        directory = Path(mkdtemp())
        for document in documents:
            if self._canceled():
                break
            path = directory / self._unique_name(document.name or f"{FALLBACK_NAME}.json", taken)
            self.write_json(document.data, path)
            files.append(LocalFile(path=str(path), mime="application/json"))
            self._report(len(files), "write", "JSON file returned", "JSON files returned")
        return Entities(entities=iter([schema.to_entity(_) for _ in files]), schema=schema)

    def _provide_output(self, documents: list[Document]) -> Entities:
        """Depending on configuration, provide the parsed documents in different shapes"""
        try:
            # Select a _provide_output_* function based on target_mode
            provide_output = getattr(self, f"_provide_output_{self.target_mode}")
        except AttributeError as error:
            raise ValueError(f"Target mode not implemented yet: '{self.target_mode}'") from error
        entities: Entities = provide_output(documents)
        return entities

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities:
        """Execute the workflow plugin on a given sequence of entities"""
        self.log.info("start execution")
        self.execution_context = context
        self.skipped = []
        self.reported = (0, "documents parsed")
        self.canceled = False
        return self._provide_output(self._get_input(inputs))

    @staticmethod
    def _as_text(value: object) -> str:
        """Render a value the way a document which disagreed about it would read"""
        if isinstance(value, dict | list):
            return json.dumps(value, default=str)
        return str(value)

    @staticmethod
    def _shape(value: object) -> str:
        """Name the shape a value has, as far as one entity schema is concerned"""
        if isinstance(value, dict):
            return "mapping"
        if isinstance(value, list):
            return "list"
        return "value"

    @classmethod
    def _shapes(cls, documents: list) -> dict[str, set[str]]:
        """Collect the shapes each key takes across a list of mappings"""
        shapes: dict[str, set[str]] = {}
        for document in documents:
            for key, value in document.items():
                shapes.setdefault(str(key), set()).add(cls._shape(value))
        return shapes

    @classmethod
    def _coerce_conflicting_values(cls, documents: list) -> list:
        """Render a key as text in every document as soon as they disagree about its shape.

        Entities carry one schema for all of them, so a key which is a string in one
        document and a list in the next has to be one or the other. Left alone, the entity
        builder lets the last document decide and re-reads the string one character per
        value; as text, both documents keep what they said.
        """
        if not all(isinstance(_, dict) for _ in documents):
            return documents
        coerced = [dict(_) for _ in documents]
        for key, found in cls._shapes(coerced).items():
            carrying = [_ for _ in coerced if key in _]
            if len(found) > 1:
                for document in carrying:
                    document[key] = cls._as_text(document[key])
            elif found == {"mapping"}:
                nested = cls._coerce_conflicting_values([_[key] for _ in carrying])
                for document, value in zip(carrying, nested, strict=True):
                    document[key] = value
        return coerced

    @classmethod
    def _stringify_keys(cls, value: object) -> object:
        """Give every mapping key its string form, so that JSON and entities can carry it.

        YAML keys are not necessarily strings: `on:` is the boolean True under YAML 1.1,
        `80:` is an integer and `2026-01-01:` is a date. JSON has string keys only and an
        EntityPath is a string, so the type is dropped here rather than by whoever writes
        the value out - quote such a key in the source document to keep it as it reads.
        """
        if isinstance(value, dict):
            stringified: dict[str, object] = {}
            for key, item in value.items():
                text = str(key)
                if text in stringified:
                    raise ValueError(
                        f"the key '{text}' appears twice once its YAML type is dropped, "
                        "so one of the two values would be lost"
                    )
                stringified[text] = cls._stringify_keys(item)
            return stringified
        if isinstance(value, list):
            return [cls._stringify_keys(_) for _ in value]
        return value

    @classmethod
    def parse_yaml(cls, source: IO[bytes] | str) -> dict | list:
        """Parse a YAML document from a stream or a string.

        Raises:
            TypeError: when the document is neither a mapping nor a sequence
            ValueError: when two keys collide once their YAML type is dropped
            yaml.YAMLError: when the document is not valid YAML

        """
        content = cls._stringify_keys(yaml.safe_load(source))
        if not isinstance(content, dict | list):
            raise TypeError("YAML content could not be parsed to a dict or list.")
        return content

    @staticmethod
    def write_json(data: dict | list, path: Path) -> Path:
        """Write a parsed document to a JSON file and return its path.

        YAML carries types JSON does not have - a plain `1990-01-02` is a date, not a
        string - so anything json cannot serialise is written as its string form. That is
        what the entities output does with the same value, and without it a single dated
        document would abort a whole batch.
        """
        with path.open("w", encoding="utf-8") as writer:
            json.dump(data, writer, default=str)
        return path
