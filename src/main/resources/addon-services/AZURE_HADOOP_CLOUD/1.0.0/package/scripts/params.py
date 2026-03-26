#!/usr/bin/env python3
"""
OS-agnostic parameter loader for AZURE_HADOOP_CLOUD service.
Delegates to platform-specific params module.
"""
import os
import sys

from resource_management.libraries.functions.default import default
from resource_management.libraries.script.script import Script

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

# Load OS-specific parameters
from params_linux import *  # noqa: F401,F403
