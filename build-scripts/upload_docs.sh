#!/bin/bash

cd docs/build
mv html mongomotor
tar -czf mmdocs.tar.gz mongomotor

curl --user "$TUPI_USER:$TUPI_PASSWD" -F 'file=@mmdocs.tar.gz' https://docs.poraodojuca.dev/e/
