#!/usr/bin/env bash

x=$(cat $task1_out)

echo "1>>>$x"

res=$(( $x * 2 ))

echo "2>>>$res"

echo "$res" > "$task2_out"
