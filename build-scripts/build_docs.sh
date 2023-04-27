#!/bin/bash
cd docs
export PYTHONPATH="$PWD/../"
rm -rf "$PWD/source/apidoc"
rm -rf "$PWD/build"
which pip
echo $PATH
base_cmd=""
if [ "$ENV" == "ci" ]
then
    base_cmd="../.././python-venv/venv-python3.11/bin/"
fi
ls ../.././python-venv/venv-python3.11/bin
ls /home/toxicuser/.venv/bin/pip
"$base_cmd"sphinx-apidoc -o "$PWD/source/apidoc" "$PWD/../mongomotor/"
"$base_cmd"sphinx-build -b html -d build/doctrees   source build/html/
