#!/bin/bash

cd docs/build
mv html mongomotor
tar -czf mmdocs.tar.gz mongomotor

curl -F 'file=@mmdocs.tar.gz' https://docs.poraodojuca.dev/e/ -H 'Authorization: Key $TUPI_AUTH_KEY'
