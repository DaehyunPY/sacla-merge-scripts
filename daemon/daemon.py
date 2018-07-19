# %% import external dependencies
from glob import glob
from os import remove
from os.path import basename, exists, getmtime, dirname
from typing import List, Set, Mapping
from time import sleep
from datetime import datetime, timedelta
from threading import Thread, active_count
from itertools import groupby
from subprocess import call
from textwrap import dedent
from tempfile import NamedTemporaryFile

from importlib_resources import path

from . import rsc


__all__ = ['run']


# %% parameters
maxworkers = 64
startinterval = 30


def workingfile(key: str) -> str:
    """
    Working dir where a preanalyzing process works.
    """
    return f'/path/to/{key}.parquet'


def keypatt(filename: str) -> str:
    """
    A rule (pattern) getting keys from lma filenames, where key is unique str indentifying lma file groups.
    In other words, files having the same key belong to the same group. For example, if we define the function as...

    keypatt(filename: str) -> str:
        from os.path import basename
        key, _ = basename(filename).rsplit('__', maxsplit=1)
        return key
    
    then, these two files 'aq001__0000.lma' and 'aq001__0001.lma' have the same key 'aq001'; they will be preanalyzed
    as the same lma group.
    """
    return basename(dirname(filename))


def targetlist() -> List[str]:
    return glob('/path/to/SortEvent_*.root')


# %%
def currentkeys() -> Mapping[str, float]:
    """
    Current keys (lma file groups) have to be preanalyzed and their last modifed timestamp.
    Do not return keys which already have been analyzed.
    """
    return  {k: max(getmtime(f) for f in groupped)
             for k, groupped in groupby(targetlist(), keypatt) if not exists(workingfile(k))}


def todolist() -> Set[str]:
    print(f"[{datetime.now()}] Scanning parquet files...")
    lastkeys = currentkeys()
    lastchecked = datetime.now()
    while True:
        if datetime.now() < lastchecked + timedelta(seconds=startinterval):
            sleep(startinterval)
            continue
        print(f"[{datetime.now()}] Scanning new parquet files...")
        curr = currentkeys()
        lastchecked = datetime.now()
        yield sorted(k for k in curr if k in lastkeys and curr[k] <= lastkeys[k])
        lastkeys = curr


def islocked(key: str) -> bool:
    out = workingfile(key)
    locker = f'{out}.locked'
    return exists(locker)


def quote(string: str) -> str:
    return f'"{string}"'


def work(key: str) -> None:
    out = workingfile(key)
    locker = f'{out}.locked'
    print(f"[{datetime.now()}] Working on key '{key}'...")
    with open(locker, 'w'):
        with path(rsc, 'merge.py') as exe:
            with NamedTemporaryFile('w', delete=False) as f:
                f.write(dedent("""\
                    #!/bin/bash
                    #PBS -N merge_{key}
                    #PBS -l nodes=1:ppn=14
                    #PBS -q serial
                    pipenv shell {exe} \
                        {targets} \
                        -o {out}
                    """).format(
                        key=key,
                        exe=quote(exe),
                        targets=' '.join(quote(f) for f in targetlist() if keypatt(f)==key),
                        out=quote(out),
                    )
                )
                fn = f.name
            call(["qsub", fn], cwd="/home/daehyun/sacla-merge-scripts")
    remove(fn)
    remove(locker)


# %% inf loop
def run() -> None:
    for jobs in todolist():
        for key in jobs:
            if not islocked(key) and active_count()-1 < maxworkers:
                job = Thread(target=work, args=[key])
                job.start()
