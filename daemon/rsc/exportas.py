#!/usr/bin/env python3
# %% external dependencies
from array import array
from itertools import islice
from argparse import ArgumentParser
from os import rename
from os.path import splitext

from ROOT import TFile, TTree
from pyspark.sql import SparkSession


# %% parser & parameters
parser = ArgumentParser(prog='exportas', description="Export SACLA parquet file as root.")
parser.add_argument('parquetfiles', metavar='filenames', type=str, nargs='+',
                    help='parquet file to export')
parser.add_argument('-o', '--output', metavar='filename', type=str, default='exported.root',
                    help='filename to which the data is saved')
args = parser.parse_args()
targets = args.parquetfiles
saveas = args.output
maxhits = 8


# %% initialize spark builder
builder = (SparkSession
           .builder
           .config("spark.jars.packages", "org.diana-hep:spark-root_2.11:0.1.15")
           # .config("spark.cores.max", 11)
           # .config("spark.executor.cores", 5)
           # .config("spark.executor.memory", "4g")
           )


# %% to root
with builder.getOrCreate() as spark:
    df = spark.read.parquet(*targets)
    try:
        f = TFile(saveas, 'NEW')
        tree = TTree('Events', 'Events')
        tag = array('i', [0])
        nhits = array('i', [0])
        tarr = [array('f', [0]) for _ in range(maxhits)]
        xarr = [array('f', [0]) for _ in range(maxhits)]
        yarr = [array('f', [0]) for _ in range(maxhits)]
        flagarr = [array('i', [0]) for _ in range(maxhits)]
        tree.Branch('Tag', tag, 'Tag/I')
        tree.Branch('IonNum', nhits, 'IonNum/I')
        for i in range(maxhits):
            tree.Branch(f'IonT{i}', tarr[i], f'IonT{i}/F')
            tree.Branch(f'IonX{i}', xarr[i], f'IonX{i}/F')
            tree.Branch(f'IonY{i}', yarr[i], f'IonY{i}/F')
            tree.Branch(f'IonFlag{i}', flagarr[i], f'IonFlag{i}/I')
        for d in df.toLocalIterator():
            tag[0] = d.tag
            nhits[0] = len(d.hits)
            for i, h in enumerate(islice(d.hits, maxhits)):
                tarr[i][0] = h.t
                xarr[i][0] = h.x
                yarr[i][0] = h.y
                flagarr[i][0] = h.flag
            tree.Fill()
        f.Write()
        f.Close()
    except KeyboardInterrupt:
        f.Write()
        f.Close()
        base, ext = splitext(saveas)
        rename(saveas, f'{base}-interrupted{ext}')
