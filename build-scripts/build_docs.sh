#!/bin/bash
cd docs
export PYTHONPATH="$PWD/../"
export PATH=.././python-venv/venv-python3.11/bin:/home/toxicuser/.venv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
echo "######"
echo $PATH
echo "######"
rm -rf "$PWD/source/apidoc"
rm -rf "$PWD/build"
sphinx-apidoc -o "$PWD/source/apidoc" "$PWD/../mongomotor/"
make html
