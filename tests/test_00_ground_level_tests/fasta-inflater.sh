#!/usr/bin/env bash

if [[ "${PLEASE_CRASH}" == "$__task_key" ]]; then
  exit 1
fi


cat $fasta_file > $inflated_output

cat $fasta_file >> $inflated_output

export huge_variable=123
