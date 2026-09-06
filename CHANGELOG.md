<!-- markdownlint-disable MD012 MD013 MD024 MD033 -->
# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Added

- source mode **file** reads files from an input port with the File Entity Schema, and
  target mode **file** writes one JSON file per document to the output port
- the task can declare more than one input port, so documents can be read from several
  tasks at once - the advanced **Number of Input Ports** parameter, read in port order
- every entity of every connected port is parsed now, one document each, instead of only
  the first one
- the advanced **Tolerate Unusable Input** parameter skips a document which cannot be
  parsed and turns an input which delivers nothing into an empty result; a batch in which
  every document fails stays an error

### Removed

- the **file** source mode no longer reads a project file picked in the task - the
  **YAML File** parameter is gone, and a file arrives on the input port instead
- target mode **json_dataset** and its **Target Dataset** parameter - write the file
  output to a project resource with a task which stores files
- target mode **json_entities** - use the file output for the JSON document

### Changed

- updated dependencies and template
- reworked the task documentation and the parameter descriptions of **Parse YAML**
    - the source and target modes are now explained in the dropdown labels only,
      instead of being listed a second time in the task documentation
    - the documentation describes which ports exist in which mode, and names the
      behaviour users stumble over: a document has to be a mapping or a sequence,
      only one document per file is read, and documents leaving as entities share
      one unioned schema
- the target mode dropdown lists **entities** first, which is the mode the task
  starts with
- documents leaving as entities are combined into one stream whose paths are the union of
  all of them, and a value a document does not carry becomes empty
- a returned file is named after the file it came from, with a `.json` suffix, and is
  made unique when two documents would otherwise share a name - two input ports
  delivering the same file name used to produce two results a downstream task could
  not tell apart
- `cmem-client` is no longer a dependency of this package: files are read through the File
  Entity Schema of `cmem-plugin-base`, which talks to the deployment itself
- **Parse YAML** logs a warning for an input port which delivered nothing, and for
  each document it skipped, instead of dropping input in silence
- the execution report counts what it describes in the singular when there is one of
  it, and says a JSON file is *returned* rather than *written*, since the task hands
  it on rather than storing it

### Fixed

- a document holding a value JSON has no type for - a plain `1990-01-02` is a date, not
  a string - no longer aborts the whole batch with an unreported error; it is written as
  its string form, which is what the entities output already did with it
- a batch mixing a mapping and a list of plain values reports what is wrong instead of
  failing deep inside the entity builder
- a compressed file is decompressed on the way in, instead of being reported as invalid
  YAML
- a file which cannot be read reports and skips for the reasons which actually occur - a
  corrupt archive and an error from the deployment are not `OSError` - and a malformed
  file entity no longer bypasses **Tolerate Unusable Input**
- a skipped document names the file it came from and why, rather than the name of a JSON
  file which was never written; the warnings of the execution report carry the reason too
- cancelling a run no longer reports that the input port delivered nothing, and no
  further files are written once it is cancelled
- inputs beyond the declared **Number of Input Ports** are logged instead of dropped in
  silence
- an empty result is reported, so a tolerated empty batch is not indistinguishable from a
  task which never ran
- the fallback file name of a single document is `parsed-yaml.json` again; it only gains a
  number when another document would take the same name
- all documents of a run share one temporary directory instead of one each
- the source mode of a task built in Python is **code**, the same mode the task
  starts with in the workflow editor - the constructor still defaulted to
  **entities**
- an entity carrying no value at all now reports that no value is available,
  and points at the Input Schema Path / Property, instead of failing with a bare
  `StopIteration`
- a YAML document holding a list of plain values no longer produces an empty result while
  declaring an output port - it reports that there is nothing to build entities from

## [1.1.1] 2026-08-19

### Fixed

- Bumped cmem-client to 1.0.0 for DI compatability

## [1.1.0] 2026-08-05

### Changed

- Updated template and dependencies
- Exchanged all `cmempy` related code with `cmem-client` code

## [1.0.1] 2025-12-09

### Fixed

- Documentation renders bullet points properly

### Changed

- Upgrade template for trivy use

## [1.0.0] 2024-10-15

### Changed

- Update of dependencies and template
- validation of python 3.13 compatibility


## [0.8.0] 2024-07-17

### Changed

- Update of dependencies


## [0.7.0] 2024-02-16

### Added

- output as plain entities


## [0.6.0] 2023-11-29

### Added

- advanced options to control input schema (type + property)
- better error messages for cases with missing data (no entity, no value)


## [0.5.0] 2023-11-16

### Added

- Initial version using pyaml for parsing
- Source modes:
  - entities: Content is parsed from of the input port in a workflow (default)
  - code: Content is parsed from the YAML code field below
  - file: Content is parsed from an uploaded project file resource (advanced option)
- Target modes:
  - json_entities: Parsed structure will be sent as JSON entities to the output port (current default)
  - json_dataset: Parsed structure will be is saved in a JSON dataset (advanced option)

