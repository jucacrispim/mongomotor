#!/bin/sh

$1coverage run --source=mongomotor setup.py test --test-suite=tests -q
coverage=`$1coverage report -m | grep TOTAL | sed 's/TOTAL\s*\w*\s*\w*\s*//g' | cut -d'%' -f1`

echo '#######'
echo 'coverage was:' $coverage '%'
echo '#######'

if [ $coverage -eq 100 ]
then
    exit 0
else
    coverage report -m
    exit 1
fi
