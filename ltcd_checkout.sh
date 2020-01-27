#!/bin/bash

LTCD_COMMIT=$(cat go.mod | \
        grep github.com/ltcsuite/ltcd | \
        tail -n1 | \
        awk -F " " '{ print $2 }' | \
        awk -F "/" '{ print $1 }')
echo "Fetching ltcd at $LTCD_COMMIT"

pushd /tmp
GO111MODULE=on go get -v github.com/ltcsuite/ltcd@$LTCD_COMMIT
popd
