#!/usr/bin/env python3
"""Entry-point script to launch the local Options & Greeks Visualization Web App."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from options_data.viewer import main

if __name__ == "__main__":
    sys.exit(main())
