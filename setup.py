from setuptools import setup

# Project metadata and dependencies live in pyproject.toml (PEP 621).
# This shim exists only for tooling that still invokes setup.py directly;
# setuptools reads the configuration from pyproject.toml.
setup()
