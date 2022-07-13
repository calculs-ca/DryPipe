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

echo "x=9876" > $__output_var_file
echo "s1='abc'" >> $__output_var_file

# export s2=... not exporting, since may_be_none:  s2=dp.Var(type=str, may_be_none=True)

echo "that was fancy !"