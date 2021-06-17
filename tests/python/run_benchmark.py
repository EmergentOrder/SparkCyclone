from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark import StorageLevel

import argparse
import sys
from timeit import default_timer as timer

from column_operation_dict import operations, aggregate

def arguments():
    args = argparse.ArgumentParser(description='Run Benchmark. Please generate dataset using generate_data.py first.')
    args.add_argument('inputfile', type=str, metavar='file_url', help='Input file URL')
    args.add_argument('-r','--repartitions', type=int, default=200, help='Number of partitions')
    args.add_argument('-o','--outputfile', type=str, default=None, help='Output file name (CSV)')
    args.add_argument('-x','--executor', type=str, default='18g', help='Set Executor Memory')
    args.add_argument('-d','--driver', type=str, default='3g', help='Set Driver Memory')
    args.add_argument('-sl','--storageLevel', type=str, default='11001', help='Set Storage Level')
    args.add_argument('-t','--type', type=str, default='groupbyagg', help='Set Benchmark Type', choices=['groupbyagg','repart','innerjoin','broadinnerjoin', 'column'])
    args.add_argument('-n','--ntest', type=int, default=5, help='Number of Tests')
    args.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)
    args.add_argument('-cc', '--clearcache', action='store_true', default=False, help="Clear cache for every tasks")

    return args.parse_args()

def groupby_agg_benchmark(df, log, spark, args):
    res = ['groupbyagg']

    for i in range(args.ntest):
        log.info("="*64)
        log.info('Starting Benchmark for GroupBy & Agg')
        spark.sparkContext.setLocalProperty('callSite.short', f'groupby_agg_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()
        res_df = (df.groupBy('prefix2').agg(
            F.count('*').alias('total_count'),
            F.countDistinct('prefix4').alias('prefix4_count'),
            F.countDistinct('prefix8').alias('prefix8_count'),
            F.sum('float_x').alias('float_val_sum'),
            F.sum('int_x').alias('integer_val_sum'),
        ))

        log.info(f'Count value for groupby_agg_benchmark() = {res_df.rdd.count()}')
        time_taken = timer() - start_time
        res_df.explain()
        log.info(f'Running for groupby_agg_benchmark_test_{i} = {time_taken}')
        res.append(time_taken)
    
    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    log.info(f'Avg for groupby_agg_benchmark_test = {avg}')
    log.info("="*64)

    res.append(avg)
    
    return [tuple(res)]

def repartition_benchmark(df, log, spark, args):
    res = ['repart']

    for i in range(args.ntest):
        log.info("="*64)
        log.info('Starting Benchmark for Repartition')
        spark.sparkContext.setLocalProperty('callSite.short', f'repartition_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()
        res_df = (df.repartition(args.repartitions,'prefix4'))

        log.info(f'Count value repartition_benchmark() = {res_df.rdd.count()}')
        log.info(f'Number of partition after repartition_benchmark() = {res_df.rdd.getNumPartitions()}')

        time_taken = timer() - start_time
        res_df.explain()
        log.info(f'Running for repartition_benchmark_{i} = {time_taken}')
        res.append(time_taken)
    
    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    log.info(f'Avg for repartition_benchmark = {avg}')
    log.info("="*64)

    res.append(avg)

    return [tuple(res)]

def innerjoin_benchmark(df, log, spark, args):
    res = ['innerjoin']

    for i in range(args.ntest):
        log.info("="*64)
        log.info('Starting Benchmark for Inner Join')
        spark.sparkContext.setLocalProperty('callSite.short', f'innerjoin_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()

        df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
        res_df = (df.join(df1, on='prefix2', how='inner'))

        log.info(f'Count value for innerjoin_benchmark() = {res_df.rdd.count()}')

        time_taken = timer() - start_time
        res_df.explain()
        log.info(f'Running for innerjoin_benchmark_{i} = {time_taken}')
        res.append(time_taken)

    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    log.info(f'Avg for innerjoin_benchmark = {avg}')
    log.info("="*64)

    res.append(avg)
    
    return [tuple(res)]

def broadcast_innerjoin_benchmark(df, log, spark, args):
    res = ['broadinnerjoin']
    
    for i in range(args.ntest):
        log.info("="*64)
        log.info('Starting Benchmark for Broadcast Inner Join')
        spark.sparkContext.setLocalProperty('callSite.short', f'broadcast_innerjoin_benchmark()_test_{i}')

        if (args.clearcache):
            spark.catalog.clearCache() 

        start_time = timer()

        df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
        res_df = (df.join(F.broadcast(df1), on='prefix2', how='inner'))

        log.info(f'Count value for broadcast_innerjoin_benchmark() = {res_df.rdd.count()}')

        time_taken = timer() - start_time
        res_df.explain()
        log.info(f'Running for broadcast_innerjoin_benchmark_{i} = {time_taken}')
        res.append(time_taken)

    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    log.info(f'Avg for broadcast_innerjoin_benchmark = {avg}')
    log.info("="*64)

    res.append(avg)
    return [tuple(res)]
    
def column_benchmark(df, log, spark, args):
    res = []
    dicts = {**operations, **aggregate}
    ops = dicts.keys() if args.list is None else args.list
    
    log.info("="*64)
    log.info(f'Starting Benchmark for column_benchmark() : {str(ops)}')

    for op in ops:
        col_op = [op]

        try:
            for i in range(args.ntest):
                spark.sparkContext.setLocalProperty('callSite.short', f'{op}_benchmark_test_{i}')

                if (args.clearcache):
                    spark.catalog.clearCache() 

                start_time = timer()
                new_df = dicts[op](df)
                new_df.write.csv(
                    f'temp/{op}_{i}',
                    header=True,
                    mode='overwrite'
                )
                time_taken = timer() - start_time
                new_df.explain()
                log.info(f'Running {op}_benchmark_test_{i} = {time_taken}')
                col_op.append(time_taken)

            avg = (sum(col_op[1:]) - max(col_op[1:]) - min(col_op[1:])) / (args.ntest-2)
            log.info(f'Avg for {op}_benchmark_test = {avg}')

            col_op.append(avg)
            res.append(tuple(col_op))

        except Exception as e:
            log.info(f'Error {str(e)}. Skipping operation {op}')
    
    log.info("="*64)

    return res

def main(args):
    appName = f'{args.type}_benchmark'
    conf = SparkConf().setAll([('spark.executor.memory', args.executor), ('spark.driver.memory',args.driver)]) 
    spark = SparkSession.builder.appName(appName).config(conf=conf).getOrCreate()

    logger = spark._jvm.org.apache.log4j.Logger
    log = logger.getLogger(__name__)

    log.info("="*64)
    log.info(f'Shuffle Benchmark using input file = {args.inputfile} with storage level = {args.storageLevel}')
    log.info("="*64)

    callSiteShortOrig = spark.sparkContext.getLocalProperty('callSite.short')

    schema = T.StructType([
        # T.StructField("value", T.StringType()),
        # T.StructField("prefix2", T.StringType()),
        # T.StructField("prefix4", T.StringType()),
        # T.StructField("prefix8", T.StringType()),
        T.StructField("float_x", T.DoubleType()),
        T.StructField("float_y", T.DoubleType()),
        T.StructField("int_x", T.LongType()),
        T.StructField("int_y", T.LongType()),
        T.StructField("float_a", T.DoubleType()),
        T.StructField("float_b", T.DoubleType()),
        T.StructField("int_a", T.LongType()),
        T.StructField("int_b", T.LongType()),
        # T.StructField("randn", T.LongType()),
        # T.StructField("randn1", T.DoubleType()),
        # T.StructField("degree", T.LongType()),
        # T.StructField("small_int", T.LongType()),
    ])

    level = list(tuple(args.storageLevel))
    for i in range(4):
        level[i] = bool(int(level[i]))
    level[4] = int(level[4])

    # initialize df
    df = None
    
    if "csv" in args.inputfile:
        df = spark.read.csv(args.inputfile, header=True, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "json" in args.inputfile:
        df = spark.read.json(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "parquet" in args.inputfile:
        df = spark.read.parquet(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))

    assert df, (f"Filetype not found in {args.inputfile}! Ensure that the path dir is correct.")

    result = None

    if(args.type == 'groupbyagg'):
        result = groupby_agg_benchmark(df, log, spark, args)

    elif(args.type == 'repart'):
        result = repartition_benchmark(df, log, spark, args)

    elif(args.type == 'innerjoin'):
        result = innerjoin_benchmark(df, log, spark, args)

    elif(args.type == 'broadinnerjoin'):
        result = broadcast_innerjoin_benchmark(df, log, spark, args)

    elif(args.type == 'column'):
        result = column_benchmark(df, log, spark, args)

    spark.sparkContext.setLocalProperty('callSite.short', callSiteShortOrig)
    
    log.info("="*64)
    log.info('RESULTS')
    log.info(f'Test Run = {appName}, StorageLevel = {args.storageLevel}, Operation = {args.type}.')

    if args.outputfile is not None:
        log.info(f'Writing results to {args.outputfile}_{args.storageLevel}_{args.type}')
        log.info("="*64)
        
        schema = ['test']
        for i in range(args.ntest):
            schema.append(f'test_{i}')
        schema.append('mean_exclude_max_and_min')

        results_df = spark.createDataFrame(result, schema).coalesce(1)
        results_df.write.csv(
            f'{args.outputfile}_{args.storageLevel}_{args.type}',
            header=True,
            mode='overwrite'
        )

if __name__ == '__main__':
    args = arguments()
    main(args)
    