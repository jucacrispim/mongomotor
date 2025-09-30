#!/bin/bash

confpath=~/.pypirc
echo "[pypi]\nusername = $PYPI_USERNAME\npassword = $PYPI_PASSWORD" > $confpath
cd dist
fname=`ls | grep tar`
twine upload -r pypi $fname
r=$?
rm ~/.pypirc
exit r
