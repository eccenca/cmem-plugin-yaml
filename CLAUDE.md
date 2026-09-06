# CLAUDE.md

Project specific notes for `cmem-plugin-yaml`. The general rules for any
project rendered from the `cmem-plugin-template` live in `.claude/rules/`, and
the how-to guidance lives in the `.claude/skills/` shipped with the template -
`plugin-implementation`, `plugin-documentation` and `plugin-testing`. This file
only records what is true of *this* plugin.

## What the package provides

One workflow task, `ParseYaml` in `cmem_plugin_yaml/parse.py`, registered as
`cmem_plugin_yaml-parse`. It reads YAML from one of three sources, converts each
document to JSON, and hands the result on in one of two shapes. Everything else
in the package is the icon (`logo.svg`) and an empty `__init__.py`.

## The mode matrix is the design

`source_mode` and `target_mode` are `ChoiceParameterType` parameters whose
allowed values live in the `SOURCE` and `TARGET` namespaces at the top of
`parse.py`. Those two values drive three separate things, and all three have to
stay in step whenever a mode is added, removed or renamed:

- **Dispatch is by name, not by branch.** `_get_input()` and `_provide_output()`
  look up `_get_input_<source_mode>` and `_provide_output_<target_mode>` with
  `getattr`. A new mode is a new method following that naming, plus an entry in
  the corresponding `OrderedDict`; there is no dispatch table to edit. The
  flipside is that a typo in a mode name only surfaces at execution time, as the
  `Source mode not implemented yet` / `Target mode not implemented yet`
  `ValueError`.
- **Ports depend on the modes.** `_set_ports()` maps each mode to a port
  declaration - `code` takes no input port, `entities` takes ports with a fixed
  schema built from `input_schema_type` / `input_schema_path`, `file` takes ports
  with the `FileEntitySchema`, and the target decides between an
  `UnknownSchemaPort` and a `FixedSchemaPort` on the `FileEntitySchema`.
  Corporate Memory reads these when the task is configured, so a mode without a
  port case breaks the workflow editor rather than the execution.
- **The `OrderedDict` values are user visible.** They are the option labels
  rendered in the dropdown, and they are the only place a mode is explained -
  the plugin `documentation` block deliberately does not repeat them. A new
  mode therefore needs a label that explains it, not a note somewhere else.

The number of input ports is itself a parameter. `number_of_inputs` multiplies
whichever port the source mode declares, and `_get_input_entities()` and
`_get_input_file()` walk `range(self.number_of_inputs)` rather than the length
of `inputs`, so a port which was declared but left unconnected is treated as an
empty one instead of raising an `IndexError`.

## Everything is a batch

Both port modes read **every** entity of **every** declared port, one document
each, and the code field contributes exactly one. From there the target decides
how the collected documents become the single `Entities` object `execute()` may
return:

- `file` writes one JSON file per document, each into its own `mkdtemp()`, so
  two documents of the same name cannot overwrite one another.
- `entities` hands *all* documents to `build_entities_from_data` in one call,
  which unions the paths across them and pads a missing value with `""`.
  Documents which are themselves lists are flattened into that collection.

`build_entities_from_data` returns `None` rather than raising when there is
nothing to build entities from - a list of plain values, or an empty
collection. `_provide_output_entities()` has to check for it.

## Files are read through cmem-plugin-base, never with a client of our own

The `file` source mode reads `FileEntitySchema` entities and calls
`file.read_stream(context=self.execution_context)`. **Passing `context` is not
optional**: `ProjectFile.read_stream()` falls back to the deprecated `cmempy`
whenever the context is `None`. Because the File Entity Schema does the talking,
this package has no `cmem-client` dependency at all - do not add one back
without a use that the schema cannot cover.

The task never writes to a deployment. The `file` output is a `LocalFile` in a
temporary directory, and storing it is a downstream task's job; a task which
uploads project resources names the resource after the file's basename, which is
why the naming rule below matters.

## Naming a written file

An output file is named after the file it came from, `a.yml` becoming `a.json`
via `with_suffix`. Documents which came from the code field or from an entity
value have no name of their own and fall back to `FALLBACK_NAME`, numbered when
there is more than one. `Document.name` carries `None` for exactly that case.

Names are then made unique across the whole run by `_unique_name()`, which
appends `-2`, `-3` and so on before the suffix. This is not about the file
system - every file gets its own `mkdtemp()` - but about what happens after the
task: a task which stores what this one returns names the resource after the
file's basename, so two ports delivering `alice.yml` would otherwise produce two
resources called `alice.json`, the second overwriting the first.

## Errors, and what tolerating them means

`_raise_error()` reaches the user twice, as an `ExecutionReport` error and as a
`ValueError`. Use it for anything a workflow author can fix by reconfiguring,
and write the message as an instruction ("you need to select a YAML file"),
which is the phrasing the existing messages and the tests both assume.

`tolerate_unusable_input` changes what happens to an unusable document, and the
rule has one exception which is easy to lose:

- off: the first unparseable document stops the task.
- on: it is logged, counted in `skipped` and skipped.
- on, but *every* document failed: still an error. Something arrived and none of
  it could be used, which is a different situation from nothing arriving.
- an input which delivers nothing: an error when off, an empty result when on.

## `parse_yaml` and `write_json` are deliberately staticmethods

`parse_yaml` takes a stream or a string and returns a `dict` or a `list`, and
`write_json` takes an object and a path. Neither needs a plugin instance or a
deployment, and together they are the seam the offline tests use. Keep it that
way: pure YAML and JSON handling belongs there, everything that touches ports,
the report or the context belongs on the instance. Note that `parse_yaml`
rejects a document which parses to a scalar with a `TypeError`, and that
`_parse_document()` is what turns that into a reported task error.

## Tests

`tests/test_parse.py` needs no deployment: it covers `parse_yaml`, `write_json`,
the declared ports, and the configuration errors raised in the constructor. It
also asserts that the constructor defaults match the `PluginParameter` defaults,
so the two cannot drift apart.

`tests/test_workflow.py` covers the mode matrix end to end and is entirely
`@needs_cmem` - not because the plugin needs a deployment, which it no longer
does for local files, but because `TestExecutionContext` fetches a real OAuth
token when it is constructed.

The fixtures encode what they are for: `test.yml` is a frozen snapshot of an old
Taskfile of this project, and `test_parse_yaml_success` asserts on its `version`
and its task count (16), so refreshing the file means updating the assertion.
`alice.yml` and `bob.yml` carry deliberately overlapping and diverging keys, for
the union assertion. `will-be-str.yml` and `will-be-int.yml` parse to a bare
string and a bare integer, `plain-list.yml` is a list of plain values, and
`broken.yml` is not valid YAML at all.

## Dependencies

`pyyaml` (with `types-pyyaml` for mypy) is this plugin's own dependency and the
only one beyond what the template ships. Parsing goes through `yaml.safe_load`
- never `yaml.load`, since the input is user supplied.
