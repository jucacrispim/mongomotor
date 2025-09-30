#!/bin/bash

echo "[pypi]\nusername = $PYPI_USERNAME\npassword = $PYPI_PASSWORD" > ~/.pypirc
cd dist
fname=`ls | grep tar`
twine upload $fname
r=$?
rm ~/.pypirc
exit r
