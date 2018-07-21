# %% import external dependencies
from datetime import datetime
from glob import glob
from itertools import groupby
from subprocess import call
from textwrap import dedent
from threading import Thread, active_count
from time import sleep
from typing import List, Set, Mapping

from importlib_resources import path
from os import remove
from os.path import basename, exists, getmtime, dirname
from tempfile import NamedTemporaryFile

from . import rsc

__all__ = ['run']


# %% parameters
maxworkers = 64
startinterval = 30


def workingfile(key: str) -> str:
    """
    Working dir where a preanalyzing process works.
    """
    return f'/work/uedalab/2018A8038Ueda/merged_files/{key}.parquet'


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
    return glob('/work/uedalab/2018A8038Ueda/hit_files/*/SortEvent_*.root')


# %%
def currentkeys() -> Mapping[str, float]:
    """
    Current keys (lma file groups) have to be preanalyzed and their last modifed timestamp.
    Do not return keys which already have been analyzed.
    """
    mtimes = {k: max(getmtime(f) for f in groupped) for k, groupped in groupby(targetlist(), keypatt)}
    return {k: m for k, m in mtimes.items()
            if not exists(workingfile(k)) or getmtime(workingfile(k)) < m}


def todolist() -> Set[str]:
    print(f"[{datetime.now()}] Scanning root files...")
    lastkeys = currentkeys()
    sleep(startinterval)
    while True:
        print(f"[{datetime.now()}] Scanning new root files...")
        curr = currentkeys()
        yield sorted(k for k in curr if k in lastkeys and curr[k] <= lastkeys[k])
        lastkeys = curr
        sleep(startinterval)


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
                    {exe} \
                        {targets} \
                        -o {out}
                    """).format(
                        exe=quote(exe),
                        targets=' '.join(quote(f) for f in targetlist() if keypatt(f)==key),
                        out=quote(out),
                    )
                )
                fn = f.name
            call(["/bin/bash", fn])
    remove(fn)
    remove(locker)


# %% inf loop
def run() -> None:
    for jobs in todolist():
        print(f"[{datetime.now()}] Todo list: {' '.join(jobs)}")
        for key in jobs:
            if not islocked(key) and active_count()-1 < maxworkers:
                job = Thread(target=work, args=[key])
                job.start()
