#!/usr/bin/env bash

set -e

if [ -z "$blast_result" ]; then
  echo "yees !"
else
  echo "expected file $blast_result to exist" >&2
fi

echo "v1_for_validation:$v1_from_blast" > $var_dump
echo "v2_for_validation:$v2_from_blast" >> $var_dump

echo "t1"
echo "Ultra fancy report" > $fancy_report

export x=9876
export s1='abc'

# export s2=... not exporting, since may_be_none:  s2=dp.Var(type=str, may_be_none=True)

echo "that was fancy !"