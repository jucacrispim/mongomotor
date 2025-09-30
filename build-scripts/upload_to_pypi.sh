#!/bin/bash

confpath=~/.pypirc
echo $confpath
echo "[pypi]\nusername = $PYPI_USERNAME\npassword = $PYPI_PASSWORD" > $confpath
cat $confpath
cd dist
fname=`ls | grep tar`
twine upload $fname
r=$?
rm ~/.pypirc
exit r
