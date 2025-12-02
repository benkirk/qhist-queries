#!/bin/bash

subcommands=(
  job-sizes
  job-waits
  cpu-job-sizes
  cpu-job-waits
  cpu-job-durations
  gpu-job-sizes
  gpu-job-waits
  gpu-job-durations
  pie-user-cpu
  pie-user-gpu
  pie-proj-cpu
  pie-proj-gpu
  pie-group-cpu
  pie-group-gpu
  usage-history
)

for start in 2024-12-01 2025-11-01; do
    for cmd in "${subcommands[@]}"; do
        echo $cmd
        qhist-report resource --machine derecho --start-date ${start} --end-date 2025-11-30 ${cmd}
    done
done
