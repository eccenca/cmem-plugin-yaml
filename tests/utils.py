"""Testing utilities.

The context classes needed for testing are provided by cmem_plugin_base.testing.
"""

import os

import pytest

# check for cmem environment and skip if not present
needs_cmem = pytest.mark.skipif(
    os.environ.get("CMEM_BASE_URI", "") == "", reason="Needs CMEM configuration"
)
