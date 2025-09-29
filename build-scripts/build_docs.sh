#!/bin/bash
cd docs
export PYTHONPATH="$PWD/../"
rm -rf "$PWD/source/apidoc"
rm -rf "$PWD/build"
echo $PATH
sphinx-apidoc -o "$PWD/source/apidoc" "$PWD/../mongomotor/"
sphinx-build -b html -d build/doctrees   source build/html/
