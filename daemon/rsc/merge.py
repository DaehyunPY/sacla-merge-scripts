#!/usr/bin/env python3
# %% external dependencies
from argparse import ArgumentParser

from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from saclatools import scalars_at, SpkHits

# %% parser & parameters
parser = ArgumentParser(prog='merge', description="Merge resorted root files and SACLA meta data.")
parser.add_argument('rootfiles', metavar='filenames', type=str, nargs='+',
                    help='resorted root files to be merge with SACLA meta data')
parser.add_argument('-o', '--output', metavar='filename', type=str, default='merged.parquet',
                    help='filename to which the merged data is saved')
args = parser.parse_args()
targets = args.rootfiles
saveas = args.output

hightag = 201704
equips = {  # must be correct for metadata retrieval
    'fel_status': ('xfel_mon_ct_bl1_dump_1_beamstatus/summary', bool),
    'fel_shutter': ('xfel_bl_1_shutter_1_open_valid/status', bool),
    'laser_shutter': ('xfel_bl_1_lh1_shutter_1_open_valid/status', bool),
    'fel_intensity': ('xfel_bl_1_tc_gm_1_pd_fitting_peak/voltage', float),
    'delay_motor': ('xfel_bl_1_st_4_motor_22/position', float)
}


# %% initialize spark builder
builder = (SparkSession
           .builder
           .config("spark.jars.packages", "org.diana-hep:spark-root_2.11:0.1.15")
           # .config("spark.cores.max", 11)
           # .config("spark.executor.cores", 5)
           # .config("spark.executor.memory", "4g")
           )
spark = builder.getOrCreate()


# %%
@udf(SpkHits)
def combine_hits(xarr, yarr, tarr, flagarr):
    return [{'x': x,
             'y': y,
             't': t,
             'flag': f
             } for x, y, t, f in zip(xarr, yarr, tarr, flagarr)]


chits = col('SortedEvent.fDetektors')[0]['fDetektors_fHits']


# %% to parquet
with builder.getOrCreate() as spark:
    loaded = (spark.read.format("org.dianahep.sparkroot").load(fn) for fn in targets)
    df = reduce(DataFrame.union, loaded)
    restructed = (
        df
            .withColumn('hits', combine_hits(chits.getField('fX_mm'),
                                             chits.getField('fY_mm'),
                                             chits.getField('fTime'),
                                             chits.getField('fRekmeth')))
            .select(col('SortedEvent.fEventID').alias("tag"), 'hits')
    )
    tags = restructed.select('tag').toPandas()['tag']
    meta = spark.createDataFrame(
        scalars_at(*tags, hightag=hightag, equips=equips)
            .rename_axis('tag')
            .reset_index()
    )
    merged = restructed.join(meta, restructed['tag'] == meta['tag'], 'inner')
    (
        merged
            .write
            .option("maxRecordsPerFile", 10000)  # less than 10 MB assuming a record of 1 KB,
            .parquet(saveas)
    )
