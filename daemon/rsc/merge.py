#!/usr/bin/env python3
# %% external dependencies
from glob import iglob
from os.path import splitext, basename, getmtime, getctime
from argparse import ArgumentParser

from tqdm import tqdm
from numpy import stack
from h5py import File
from pyspark.sql import SparkSession

from saclatools import scalars_at, LmaReader


# %% parser & parameters
parser = ArgumentParser(prog='merge', description="Merge resorted root files and SACLA meta data.")
parser.add_argument('rootfiles', metavar='filenames', type=str, nargs='+',
                    help='resorted root files to be merge with SACLA meta data')
parser.add_argument('-o', '--output', metavar='filename', type=str, default='merged.parquet',
                    help='filename to which the merged data is saved')
args = parser.parse_args()
targets = args.rootfiles
saveas = args.output

lma_filename = "/UserData/uedalab/work.uedalab/2017B8050/lma_files/{}.lma".format
hdf_filename = "/work/uedalab/2017B8050/hdf_files/{}.h5".format
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


# %% to parquet
with builder.getOrCreate() as spark:
    print("Reading lma file...")
    with LmaReader(ifile) as r:
        keys = tuple("channel{}".format(i) for i in r.channels)
        data: "List[dict]" = tuple(d for d in r)

    print("Getting SACLA metadata...")
    tags = (d['tag'] for d in data)
    meta = scalars_at(*tags, hightag=hightag, equips=equips)  # get SACLA meta data

    print("Writing hdf file...")
    with File(ofile) as f:
        f['tags'] = meta.index.values
        for k in keys:
            if k in {'channel7'}:
                continue
            f[k] = stack(tuple(d[k] for d in data)).astype('float32')
        for k in meta:
            f[k] = meta[k]
    print("Done!")

    lmas = {splitext(basename(fn))[0]: getmtime(fn) for fn in iglob(lma_filename("*"))}
    hdfs = {splitext(basename(fn))[0]: getctime(fn) for fn in iglob(hdf_filename("*"))}
    jobs = sorted(fn for fn in lmas if fn not in hdfs or hdfs[fn] < lmas[fn])

    for fn in tqdm(jobs):
        convert(lma_filename(fn), hdf_filename(fn))
