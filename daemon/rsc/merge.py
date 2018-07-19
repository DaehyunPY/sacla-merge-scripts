from glob import iglob
from os.path import splitext, basename, getmtime, getctime

from tqdm import tqdm
from numpy import stack
from h5py import File

from saclatools import scalars_at, LmaReader

# parameters!
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


def convert(ifile, ofile='exported.h5'):
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
