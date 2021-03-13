
import binascii
import functools
import os
from typing import Callable

import click
from tqdm import tqdm

from omnidiff.dirs import DirInfo

@click.group()
@click.version_option()
def main():
    pass

def plural(count: int, singular: str, multiple: str = None):
    if count == 1:
        return f'1 {singular}'
    if multiple is None:
        multiple = singular + 's'
    return f'{count} {multiple}'

def hash_options(
    func: Callable = None,
    *,
    populate: bool = True, resume: bool = False, fast: bool = False,
):
    def decorator(func):
        for dec in reversed((
            click.option(
                '--populate/--no-populate', default=populate,
                help=f'Search for files and hash them (default {populate}). If there is no stored data from a previous run, then --no-populate is ignored.',
            ),
            click.option(
                '--hash-empty/--no-hash-empty', default=True,
                help='Include empty files when hashing (default True).',
            ),
            click.option(
                '--fast/--complete', default=fast,
                help=f'Hash only files where there is a size match (--fast), or all files (--complete) (default --{"fast" if fast else "complete"}).',
            ),
            click.option(
                '--resume/--no-resume', default=resume,
                help=f'Re-use stored file data from previous run (default {resume}). With --populate, any files previously hashed will not be hashed again.',
            ),
            click.option(
                '--threads', default=1,
                help='Number of threads to use when hashing files (default 1).',
            ),
        )):
            func = dec(func)
        return func
    return decorator if func is None else decorator(func)


def read_dirinfo(func):
    @functools.wraps(func)
    def decorated(
        dirname: str,
        populate: bool, hash_empty: bool, fast: bool, resume: bool,
        threads: int,
    ):
        dirname = os.path.abspath(dirname)
        if resume or not populate:
            dirinfo = DirInfo.cached(dirname)
            if dirinfo.file_count == 0:
                populate = True
        else:
            dirinfo = DirInfo(dirname)
        if populate:
            dirinfo.populate(
                no_empty=not hash_empty,
                fast=fast,
                resume=resume,
                threads=threads,
                progress=tqdm,
            )
            filename = dirinfo.save()
            tqdm.write(f'Written {filename}')
        return func(dirinfo)
    return decorated

@main.command()
@click.argument('dirname')
@hash_options
@read_dirinfo
def info(dirinfo: DirInfo) -> None:
    """Read and summarize directory info."""
    files = plural(dirinfo.file_count, 'file')
    groups = plural(sum(1 for _ in dirinfo.dupe_groups()), 'dupe group')
    tqdm.write(f'Found {files} and {groups}')

@main.command()
@click.argument('dirname')
@hash_options(resume=True, fast=True, populate=False)
@read_dirinfo
def dupes(dirinfo: DirInfo) -> None:
    """Show groups of duplicate files in directory."""
    for group in dirinfo.dupe_groups():
        example = next(iter(group))
        hashcode = binascii.hexlify(example.hash).decode('ascii')
        print(f'{len(group)} duplicates with size {example.size}, hash {hashcode}')
        for name in sorted(str(file.relpath) for file in group):
            print(f'  {name}')

if __name__ == '__main__':
    main()
