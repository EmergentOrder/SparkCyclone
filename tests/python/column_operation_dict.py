import pyspark.sql.functions as F
import pyspark.sql.types as T

operations = {
    'abs' : lambda df : df.select(F.abs(df['integer_val'])),
    'acos' : lambda df : df.select(F.acos(df['randn'])),
    'acosh' : lambda df : df.select(F.acosh(df['randn'])),
    'ascii': lambda df : df.select(F.ascii(df['prefix2'])),
    'asin': lambda df : df.select(F.asin(df['randn1'])),
    'asinh' : lambda df : df.select(F.asinh(df['randn1'])), 
    'atan' : lambda df : df.select(F.atan(df['randn1'])),
    'atanh' : lambda df : df.select(F.atanh(df['randn1'])),
    'atan2' : lambda df : df.select(F.atan2(df['randn1'], df['randn'])),
    'base64' : lambda df : df.select(F.base64(df['value'])), 
    'bin' : lambda df : df.select(F.bin(df['integer_val'])),
    'bitwiseNOT' : lambda df : df.select(F.bitwiseNOT(df['integer_val'])), 
    'cbrt' : lambda df : df.select(F.cbrt(df['randn'])),
    'concat' : lambda df : df.select(F.concat(df['prefix2'], df['prefix4'])), 
    'concat_ws' : lambda df : df.select(F.concat_ws('-', df['prefix2'], df['prefix4'], df['float_val'])),
    'conv' : lambda df : df.select(F.conv(df['integer_val'], 10, 16)),
    'cos' : lambda df : df.select(F.cos(df['randn'])),
    'cosh' : lambda df : df.select(F.cosh(df['randn'])), 
    'crc32' : lambda df : df.select(F.crc32(df['value'])),
    'degrees' : lambda df : df.select(F.degrees(df['degree'])),
    'exp' : lambda df : df.select(F.exp(df['randn'])), 
    'expr' : lambda df : df.select(F.expr("length(float_val)")),
    'factorial' : lambda df : df.select(F.factorial(df['small_int'])),
    'hash' : lambda df : df.select(F.hash(df['value'])), 
    'hex' : lambda df : df.select(F.hex(df['value'])),
    'hypot' : lambda df : df.select(F.hypot(df['integer_val'], df['randn'])),
    'levenshtein' : lambda df : df.select(F.levenshtein(df['value'],df['integer_val'])), 
    'log' : lambda df : df.select(F.log(df['integer_val'])),
    'log10' : lambda df : df.select(F.log10(df['float_val'])),
    'log1p' : lambda df : df.select(F.log1p(df['randn'])), 
    'log2' : lambda df : df.select(F.log2(df['randn1'])),
    'md5' : lambda df : df.select(F.md5(df['value'])),
    'pow' : lambda df : df.select(F.pow(df['randn'], df['small_int'])),
    'radians' : lambda df : df.select(F.radians(df['degree'])),
    'sha1' : lambda df : df.select(F.sha1(df['value'])),
    'sha2' : lambda df : df.select(F.sha2(df['value'], 256)),
    'signum' : lambda df : df.select(F.signum(df['integer_val'])),
    'sin' : lambda df : df.select(F.sin(df['randn'])),
    'sinh' : lambda df : df.select(F.sinh(df['randn'])),
    'sqrt' : lambda df : df.select(F.sqrt(df['small_int'])),
    'tan' : lambda df : df.select(F.tan(df['randn'])),
    'tanh' : lambda df : df.select(F.tanh(df['randn'])),
    'xxhash64' : lambda df : df.select(F.xxhash64(df['value'])),
    'bitwiseAND' : lambda df : df.select(df['small_int'].bitwiseAND(df['integer_val'])),
    'bitwiseOR' : lambda df : df.select(df['small_int'].bitwiseOR(df['integer_val'])),
    'bitwiseXOR' : lambda df : df.select(df['small_int'].bitwiseXOR(df['integer_val'])),
    '(x+y)': lambda df : df.select(df['float_val'] + df['integer_val']),
    '(x-y)': lambda df : df.select(df['float_val'] - df['integer_val']),
    '(x*y)': lambda df : df.select(df['float_val'] * df['integer_val']),
    '(x/y)': lambda df : df.select(df['float_val'] / df['integer_val'])
}

aggregate = {
    'approx_count_distinct' : lambda df : df.agg(F.approx_count_distinct(df['integer_val'])),
    'avg' : lambda df : df.agg(F.avg(df['integer_val'])), 
    'avg(x+y)' : lambda df : df.agg(F.avg(df['integer_val'] + df['float_val'])), 
    'corr' : lambda df : df.agg(F.corr(df['float_val'], df['randn'])), 
    'count' : lambda df : df.agg(F.count(df['value'])), 
    'countDistinct' : lambda df : df.agg(F.countDistinct(df['value'], df['integer_val'])), 
    'covar_pop' : lambda df : df.agg(F.covar_pop(df['integer_val'], df['float_val'])), 
    'covar_samp' : lambda df : df.agg(F.covar_samp(df['randn'], df['float_val'])), 
    'kurtosis' : lambda df : df.agg(F.kurtosis(df['randn1'])), 
    'max' : lambda df : df.agg(F.max(df['float_val'])), 
    'mean' : lambda df : df.agg(F.mean(df['randn'])), 
    'min' : lambda df : df.agg(F.min(df['randn'])), 
    'percentile_approx' : lambda df : df.agg(F.percentile_approx('randn',[0.25,0.5,0.75], 100000)), 
    'skewness' : lambda df : df.agg(F.skewness(df['randn'])), 
    'stddev' : lambda df : df.agg(F.stddev(df['randn1'])), 
    'stddev_pop' : lambda df : df.agg(F.stddev_pop(df['randn1'])), 
    'stddev_samp' : lambda df : df.agg(F.stddev_samp(df['randn1'])), 
    'sum' : lambda df : df.agg(F.sum(df['integer_val'])), 
    'sum(x+y)' : lambda df : df.agg(F.sum(df['integer_val'] + df['float_val'])), 
    'sumDistinct' : lambda df : df.agg(F.sumDistinct(df['integer_val'])), 
    'var_pop' : lambda df : df.agg(F.var_pop(df['small_int'])), 
    'var_samp' : lambda df : df.agg(F.var_samp(df['integer_val']))
}