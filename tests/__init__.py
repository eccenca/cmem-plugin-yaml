"""tests"""

import os
from pathlib import Path

CURRENT_FILE_PATH = Path(os.path.realpath(__file__)).parent
PROJECT_ROOT = Path(CURRENT_FILE_PATH) / ".."
FIXTURE_DIR = Path(CURRENT_FILE_PATH) / "fixtures"
