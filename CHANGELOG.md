# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

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

