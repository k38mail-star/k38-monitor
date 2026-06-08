#!/usr/bin/env python3
"""dltrace — 文件级下载进度追踪"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dltrace import main

if __name__ == "__main__":
    main()
