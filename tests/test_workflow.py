"""Test different source and target modes"""

from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.code import YamlCode

from cmem_plugin_yaml.parse import ParseYaml, SOURCE_CODE, TARGET_JSON_ENTITIES
from tests.utils import TestExecutionContext

YAML_SOURCE = YamlCode("""---
ttt: 123
""")


def test_code_to_json_entities() -> None:
    """Test source to json entities"""
    plugin = ParseYaml(
        source_mode=SOURCE_CODE,
        target_mode=TARGET_JSON_ENTITIES,
        source_code=YAML_SOURCE,
    )
    entities: Entities = plugin.execute([], TestExecutionContext())
    assert next(entities.entities).values[0]
