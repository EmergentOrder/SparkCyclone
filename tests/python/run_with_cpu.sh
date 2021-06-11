#!/bin/bash

/opt/spark/bin/spark-submit --master yarn \
--name CPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
run_benchmark.py data/1M_R1000000_P100_csv \
-x 4g -d 4g \
-o output/test_cpu_1M_clearcache \
-t column \
-l "a=a+b_int,a=a+b_float,a=a*b_int,a=a*b_float,a=a+b*x_int,a=a+b*x_float" \
-n 5 \
--clearcache

/opt/hadoop/bin/hadoop dfs -rm -r -f temp
