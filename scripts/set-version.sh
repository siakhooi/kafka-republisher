#!/bin/bash

# shellcheck disable=SC1091
source ./release.env

sed -i 'pyproject.toml' -e 's@version = .*@version = "'"$RELEASE_VERSION"'"@g'

echo "kafka-republisher $RELEASE_VERSION" > tests/expected-output/cli-version.txt
