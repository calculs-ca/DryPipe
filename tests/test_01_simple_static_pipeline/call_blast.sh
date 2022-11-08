#!/usr/bin/env bash

set -e

if [[ $dummy_env_var = "abc" ]]; then
  echo "good..."
else
  echo >&2 echo "not good, dummy_env_var not set"
fi

export v1=1111
export v2="3.14"

echo "fake blast"

cp $fake_blast_output $blast_out

echo "that was fast !"