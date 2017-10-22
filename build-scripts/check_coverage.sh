#!/bin/sh


echo "\nChecking coverage for Python code\n"
OUT=`coverage run --source=$1 --branch setup.py test --test-suite=tests.unit`;
ERROR=$?
coverage html;
coverage=`coverage report -m | grep TOTAL | sed 's/TOTAL\s*\w*\s*\w*\s*\w*\s*\w*//g' | cut -d'%' -f1`

echo 'coverage was:' $coverage'%'

if [ "$ERROR" != "0" ]
then
    if [ $coverage -eq $2 ]
    then
	echo "But something went wrong";
	echo "$OUT";
	exit 1
    else
	echo "And something went wrong"
	echo "$OUT";
	exit 1
    fi
fi

if [ $coverage -eq $2 ]
then
    echo "Yay! Everything ok!";
    exit 0;
else
    coverage report -m
    exit 1
fi
