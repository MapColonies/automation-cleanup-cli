#!/bin/bash
pip3 install setuptools setuptools_scm
VER=$(python3 setup.py --version | sed 's/+/./g')

echo $(date):automation-cleanup-cli:$VER

echo version=$VER>version.txt
echo version=$VER>>version_history.txt
echo "::set-env name=TAG_NAME::$VER"

