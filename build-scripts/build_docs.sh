#!/bin/bash
cd docs
export PYTHONPATH="$PWD/../"
echo "######"
echo $PATH
echo "######"
rm -rf "$PWD/source/apidoc"
rm -rf "$PWD/build"
sphinx-apidoc -o "$PWD/source/apidoc" "$PWD/../mongomotor/"
make html
