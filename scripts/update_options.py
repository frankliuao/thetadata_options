#!/usr/bin/env python3
"""CLI script to incrementally update or catch up missing days for downloaded options data."""

import sys
from options_data.updater import main

if __name__ == "__main__":
    sys.exit(main())
