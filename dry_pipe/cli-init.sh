#!/usr/bin/env bash


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export DRYPIPE_PIPELINE_INSTANCE_DIR=$(dirname "$SCRIPT_DIR")

function dp {

    export DRYPIPE_DP_HINT_DIR=`pwd`

    $SCRIPT_DIR/cli "$@"
}
