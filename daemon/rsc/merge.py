#!/usr/bin/env python3
# %% external dependencies
from argparse import ArgumentParser

from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from saclatools import SpkHits, scalars_at

# %% parser & parameters
parser = ArgumentParser(prog='merge', description="Merge resorted root files and SACLA meta data.")
parser.add_argument('rootfiles', metavar='filenames', type=str, nargs='+',
                    help='resorted root files to be merged')
parser.add_argument('-o', '--output', metavar='filename', type=str, default='merged.parquet',
                    help='filename to which the merged data is saved')
args = parser.parse_args()
rootfiles = args.rootfiles
saveas = args.output

hightag = 201802
equips = {  # must be correct for metadata retrieval
    'fel_status': ('xfel_mon_bpm_bl3_0_3_beamstatus/summary', bool),
    'fel_shutter': ('xfel_bl_3_shutter_1_open_valid/status', bool),
    'laser_shutter': ('xfel_bl_3_lh1_shutter_1_open_valid/status', bool),
    'delay_motor_st4': ('xfel_bl_3_st_4_motor_25/position', int),
    'delay_motor_st1': ('xfel_bl_3_st_1_motor_73/position', int),
}
tmafiles = ["/work/uedalab/2018A8038Ueda/tma/*.csv"]


# %% initialize spark builder
builder = (SparkSession
           .builder
           .config("spark.jars.packages", "org.diana-hep:spark-root_2.11:0.1.15")
           # .config("spark.cores.max", 11)
           # .config("spark.executor.cores", 5)
           # .config("spark.executor.memory", "4g")
           )


# %%
@udf(SpkHits)
def combine_hits(xarr, yarr, tarr, flagarr):
    return [{'x': x,
             'y': y,
             't': t,
             'flag': f
             } for x, y, t, f in zip(xarr, yarr, tarr, flagarr)]


# %% to parquet
with builder.getOrCreate() as spark:
    loaded = (spark.read.format("org.dianahep.sparkroot").load(fn) for fn in rootfiles)
    df = reduce(DataFrame.union, loaded)
    chits = col('SortedEvent.fDetektors')[0]['fDetektors_fHits']
    restructed = (
        df
            .withColumn('hits', combine_hits(chits.getField('fX_mm'),
                                             chits.getField('fY_mm'),
                                             chits.getField('fTime'),
                                             chits.getField('fRekmeth')))
            .select(col('SortedEvent.fEventID').alias("tag"), 'hits')
    )
    tma = (
        spark
            .read
            .csv(*tmafiles, header=True, inferSchema=True, comment='[')
            .select(col('tag_number').alias('tag'), col('timing_edge_fitting(pixel)').alias('tma_edge'))
    )
    tags = restructed.select('tag').toPandas()['tag']
    lasttag, *_ = tma.selectExpr('MAX(tag) AS lasttag').toPandas()['lasttag'].values

    if tags.max() <= lasttag:
        meta = spark.createDataFrame(
            scalars_at(*tags, hightag=hightag, equips=equips)
                .rename_axis('tag')
                .reset_index()
        )
        merged = (
            restructed
                .join(meta, "tag", 'inner')
                .join(tma, "tag", 'inner')
        )
        (
            merged
                .write
                .option("maxRecordsPerFile", 100000)  # less than 10 MB assuming a record of 1 KB,
                .mode('overwrite')
                .parquet(saveas)
        )
