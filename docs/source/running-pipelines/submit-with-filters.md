

```
drypipe array-summary 
array_n  job_id    pending   running  failed  completed  timeout  total  ram  cpus  walltime  account          filters
array.0  58527333  inactive
array.1  61285748  inactive
array.2  61289498  inactive
array.3  21795182  inactive
array.4  21903439  inactive
array.5  21903615  inactive
array.6  21925568  inactive
array.7  21925618  inactive
array.8  21951284  inactive
array.9  22198612       353        0       0          0        0    705  28G     5  20:00:00  rrg-xroucou_cpu  --filter-from=failed.3-1174.md:1
state         step  count
completed            4217
failed           0    398
failed           2      2
failed           3   1174
failed           7     19
ready            1    347
step-started     0      3
step-started     1      2
step-started     2      2
timed-out        0     26
timed-out        2     78
timed-out        3      3
timed-out        7      1
```


```
[maxl@rorqual4 HS-g-d]$ drypipe array-summary --filter-from=failed.3-1174.md:1
array_n  job_id    pending   running  failed  completed  timeout  total  ram  cpus  walltime  account          filters
array.0  58527333  inactive
array.1  61285748  inactive
array.2  61289498  inactive
array.3  21795182  inactive
array.4  21903439  inactive
array.5  21903615  inactive
array.6  21925568  inactive
array.7  21925618  inactive
array.8  21951284  inactive
array.9  22198612         0       24       0          0        0    705  28G     5  20:00:00  rrg-xroucou_cpu  --filter-from=failed.3-1174.md:1
state         step  count
completed             284
failed           3    392
failed           7      7
step-started     3     15
step-started     7      7
[maxl@rorqual4 HS-g-d]$ 
```

```
[maxl@rorqual4 HS-g-c]$ drypipe array-submit -k msdmo-array --stop-after-step=0 --sbatch-options="--mem=180G --cpus-per-task=40 --time=5:00:00" --py-filter="{state_name} == 'killed'"
```

```
[maxl@rorqual3 HS-g-d]$ drypipe array-submit --filter-from=failed.1-868.md:1
Will submit slurm array with 856 tasks, sbatch options:
  --account=rrg-xroucou
  --mem=32G
  --cpus-per-task=8
  --time=16:00:00
  --tmp=25G
submit ? [y/N] y
```


drypipe array-summary --filter-from=failed.1-868.md:1 --filter-from=failed.2-459.md:2,3