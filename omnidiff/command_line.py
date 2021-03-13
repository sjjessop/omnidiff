
import binascii
import functools
import os
from typing import Callable

import click
from tqdm import tqdm

from omnidiff.dirs import DirInfo, FileInfo

# See source of click.core.Group. All we do is not sort before returning, so
# that the commands in the --help text appear in the order defined, instead of
# alphabetical order.
class OrderedGroup(click.Group):
    def list_commands(self, ctx):
        return list(self.commands)

@click.group(cls=OrderedGroup)
@click.version_option()
def main():
    """
    Compare groups of files or check them for duplicates.

    Typical usage is to start by running 'info', to generate a json file which
    records files below that directory and their hashes. This is stored as a
    file named <directory>.dirinfo.json, alongside <directory> in its parent
    directory.

    Then use other commands to explore dupes within the directory, or to
    compare two directories. By default, commands other than 'info' do not
    rescan the directories, they just compare the stored data from the previous
    run.
    """

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

def list_option(name: str):
    return click.option(
        f'--list-{name}/--no-list-{name}', default=False,
        help=f'List {name} file names (default False).',
    )

@main.command()
@click.argument('old_dirname')
@click.argument('new_dirname')
@list_option('identical')
@list_option('changed')
@list_option('vanished')
@list_option('added')
@list_option('all')
def compare(old_dirname: str, new_dirname: str, list_identical: bool, list_changed: bool, list_vanished: bool, list_added: bool, list_all: bool) -> None:
    """
    Summarise changes from OLD_DIRNAME to NEW_DIRNAME. Both directories must
    previously have been scanned (by the 'info' command or otherwise).

    The names OLD and NEW assume that you're comparing two snapshots of "the
    same" collection of files, for example two backups on different dates.
    """
    identical = set()
    changed = set()
    vanished = set()
    old_dir = DirInfo.load(old_dirname)
    new_dir = DirInfo.load(new_dirname)
    for old_file in old_dir:
        rel_str = old_file._rel_str
        try:
            new_file = new_dir.get_relative(rel_str)
        except KeyError:
            vanished.add(rel_str)
            continue
        if not isinstance(old_file, FileInfo):
            raise Exception(f'No hash for {old_file.fullpath}')
        if not isinstance(new_file, FileInfo):
            raise Exception(f'No hash for {new_file.fullpath}')
        if new_file.hash == old_file.hash:
            identical.add(rel_str)
        else:
            changed.add(rel_str)
    added = (
        {file._rel_str for file in new_dir}
        .difference(file._rel_str for file in old_dir)
    )
    print('old:', old_dir.file_count)
    print('new:', new_dir.file_count)
    print('identical:', len(identical))
    print('changed: ', len(changed))
    print('vanished:', len(vanished))
    print('added:', len(added))
    if list_identical or list_all:
        print('\nidentical files:\n  ', end='')
        print('\n  '.join(sorted(identical)))
    if list_changed or list_all:
        print('\nchanged files:\n  ', end='')
        print('\n  '.join(sorted(changed)))
    if list_vanished or list_all:
        print('\nvanished files:\n  ', end='')
        print('\n  '.join(sorted(vanished)))
    if list_added or list_all:
        print('\nadded files:\n  ', end='')
        print('\n  '.join(sorted(added)))

if __name__ == '__main__':
    main()
