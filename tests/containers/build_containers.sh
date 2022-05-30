#!/usr/bin/env bash

# rm *.sif

# singularity build --force --fakeroot singularity-base-test-container.sif singularity-base-test-container.def

sudo singularity build --force singularity-test-container.sif singularity-test-container.def
sudo singularity build --force singularity-test-container.sif singularity-test-container.def
