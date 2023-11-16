# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Added

- Initial version using pyaml for parsing
- Source modes:
  - entities: Content is parsed from of the input port in a workflow (default)
  - code: Content is parsed from the YAML code field below
  - file: Content is parsed from an uploaded project file resource (advanced option)
- Target modes:
  - json_entities: Parsed structure will be sent as JSON entities to the output port (current default)
  - json_dataset: Parsed structure will be is saved in a JSON dataset (advanced option)
