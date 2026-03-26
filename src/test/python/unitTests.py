#!/usr/bin/env python3
"""Test runner for Azure Hadoop Cloud mpack unit tests."""
import sys
import unittest

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover('.', pattern='Test*.py')
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
