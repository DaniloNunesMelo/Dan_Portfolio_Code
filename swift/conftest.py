# conftest.py
# Adds the swift/ directory to sys.path so pytest can import mt103 and mx_pacs008
# without needing an install step.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
