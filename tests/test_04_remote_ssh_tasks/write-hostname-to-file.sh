#!/usr/bin/env bash

if [[ "${PLEASE_CRASH}" ]]; then
  exit 1
fi

if [[ ! -z "${name_of_pipeline_host+x}" ]]; then
  cat $name_of_pipeline_host >> $precious_output
fi

echo "hello from $(cat /etc/hostname)" >> $precious_output
