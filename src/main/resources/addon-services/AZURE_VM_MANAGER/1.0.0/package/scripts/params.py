#!/usr/bin/env python3
"""
OS-agnostic parameter loader for AZURE_VM_MANAGER service.
"""
from resource_management.libraries.script.script import Script

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

from params_linux import *  # noqa: F401,F403
