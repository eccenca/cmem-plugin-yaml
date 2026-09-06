# CLAUDE.md

Project specific notes for `cmem-plugin-yaml`. The general rules for any
project rendered from the `cmem-plugin-template` live in `.claude/rules/`, and
the how-to guidance lives in the `.claude/skills/` shipped with the template -
`plugin-implementation`, `plugin-documentation` and `plugin-testing`. This file
only records what is true of *this* plugin.

## What the package provides

One workflow task, `ParseYaml` in `cmem_plugin_yaml/parse.py`, registered as
`cmem_plugin_yaml-parse`. It reads YAML from one of three sources, converts it
to JSON, and hands the result on in one of three shapes. Everything else in the
package is the icon (`logo.svg`) and an empty `__init__.py`.

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
  declaration - `file` and `code` take no input port, `entities` takes one with
  a fixed schema built from `input_schema_type` / `input_schema_path`;
  `json_dataset` produces no output port at all. Corporate Memory reads these
  when the task is configured, so a mode without a port case breaks the
  workflow editor rather than the execution.
- **The `OrderedDict` values are user visible.** They are the option labels
  rendered in the dropdown, and the plugin `documentation` block repeats them
  as prose. Change one and change the other.

`_validate_config()` runs twice: once in `__init__`, where only the parameter
values are known, and again at the top of `execute()`, where `self.client` and
`self.project` exist. Checks that need a deployment - currently
`_source_file_exists()` - are therefore guarded by `hasattr(self, "client")`,
so that the constructor stays usable without one. Put a new deployment
dependent check behind the same guard rather than moving it out of
`_validate_config()`.

Errors raised through `_raise_error()` reach the user twice: as an
`ExecutionReport` error and as a `ValueError`. Use it for anything a workflow
author can fix by reconfiguring, and write the message as an instruction ("you
need to select a YAML file"), which is the phrasing the existing messages and
the tests both assume.

## `yaml2json` is deliberately a staticmethod

It takes a `Path` and returns a `Path`, needs no plugin instance and no
deployment, and is the seam the offline tests use. Keep it that way: pure YAML
handling belongs there, everything that touches ports, the report or the client
belongs on the instance. Note that it rejects a YAML document that parses to a
scalar with a `TypeError` - only a `dict` or a `list` converts.

## Tests

`tests/test_parse.py` covers `yaml2json` against the fixtures in
`tests/fixtures/` and needs no deployment. `tests/test_workflow.py` covers the
mode matrix end to end and is entirely `@needs_cmem`.

Its `di_environment` fixture creates the project `yaml_test_project` (the name
is in `tests/utils.py`) with a JSON dataset in it, and deletes the project on
teardown. An aborted run therefore leaves that project behind, and the next run
fails in fixture setup with `Item with id yaml_test_project already exists`
rather than in a test. Delete the leftover project in the deployment before
re-running.

The fixture files encode what they are for: `test.yml` is a frozen snapshot of
an old Taskfile of this project, and `test_success` asserts on its `version`
and its task count (16), so refreshing the file means updating the assertion.
`will-be-str.yml` and `will-be-int.yml` parse to a bare string and a bare
integer and exist purely to make `yaml2json` raise.

## Dependencies

`pyyaml` (with `types-pyyaml` for mypy) is this plugin's own dependency and the
only one beyond what the template ships. Parsing goes through `yaml.safe_load`
- never `yaml.load`, since the input is user supplied.
