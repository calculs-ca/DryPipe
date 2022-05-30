#!/usr/bin/env bash

x=$(cat $file_with_number_inside)

echo "-->$x<--"

echo "$x" > "$task1_out"
