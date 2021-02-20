
from datetime import datetime, timedelta, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import time

import pytest
from tqdm import tqdm

from omnidiff.dirs import DirInfo, DummyBar, Encoder, FileInfo
from omnidiff.file import FileStats

def tempfiles(tmpdir, file_size=1):
    """
    Create a set of files for testing. Returns a tuple of:
    * A subdirectory of the tmpdir containing the test files
    * A dictionary mapping file names to contents
    * A tuple of sets: each is the names of a group of duplicate files.
    """
    subdir = (tmpdir / 'sub')
    (subdir / 'dir').mkdir(parents=True)
    x = b'x' * file_size
    y = b'y' * file_size
    files = {
        '.hidden': x,
        'equal1': x,
        os.path.join('dir', 'equal2'): x,
        'unequal': y,
        'empty': b'',
        'big': x * 2,
    }
    for filename, content in files.items():
        (subdir / filename).write_bytes(content)
    dupes = frozenset(name for name, content in files.items() if content == x)
    return (subdir, files, (dupes,))

def test_dir_info(tmp_path):
    """
    We can get a list of files in the directory, containing suitable info.
    Unless otherwise specified, this finds and hashes everything.
    """
    file_size = 12345
    subdir, files, dupe_groups = tempfiles(tmp_path, file_size)
    dirinfo = DirInfo(subdir)
    dirinfo.populate()
    check_everything(file_size, subdir, files, dupe_groups, dirinfo)

def check_everything(file_size, subdir, files, dupes, info, no_empty=False, fast=False, is_copy=False):
    def skip_hash(duplicated_sizes, actual_size):
        # Optionally some files aren't hashed.
        return (
            (no_empty and actual_size == 0)
            or (fast and actual_size not in duplicated_sizes)
        )
    assert info.file_count == len(files) > 0
    for rel_path_str, content in files.items():
        record = info.get_relative(rel_path_str)
        assert record.basepath == subdir
        assert isinstance(record.relpath, Path)
        assert os.fspath(record.relpath) == rel_path_str
        assert record.size == len(content)
        if skip_hash([file_size], record.size):
            assert not hasattr(record, 'hash'), rel_path_str
        else:
            assert record.hash == hashlib.sha256(content).digest()
            # 10 seconds is arbitrary, but it *shouldn't* be that slow.
            assert record.when >= datetime.now(tz=timezone.utc) - timedelta(seconds=10)
    # Must notice that two files have the same hash
    dupe_groups = tuple(info.dupe_groups())
    assert len(dupe_groups) == len(dupes)
    sets = tuple(
        set(os.fspath(stats.relpath) for stats in group)
        for group in dupe_groups
    )
    assert sets == dupes
    if not is_copy:
        info.save()
        clone = DirInfo.load(subdir)
        check_everything(file_size, subdir, files, dupes, clone, no_empty, fast, is_copy=True)

@pytest.mark.parametrize('no_empty, fast', itertools.product([False, True], repeat=2))
def test_dir_info_optimisation(tmp_path, no_empty, fast):
    """
    We can reduce the number of hashes computed by excluding empty files,
    and/or only hashing when file size matches.
    """
    file_size = 123
    subdir, files, dupe_groups = tempfiles(tmp_path, file_size)
    dirinfo = DirInfo(subdir)
    dirinfo.populate(no_empty=no_empty, fast=fast)
    check_everything(file_size, subdir, files, dupe_groups, dirinfo, no_empty, fast)

def test_resume(tmp_path):
    """
    The 'resume' option to populate() avoids re-calculating hashes.
    """
    file_size = 123
    subdir, files, dupe_groups = tempfiles(tmp_path, file_size)
    empty_files = set(file for file, content in files.items() if len(content) == 0)
    assert 0 < len(empty_files) < len(files)
    non_empty_files = set(files) - empty_files
    dirinfo = DirInfo(subdir)
    dirinfo.populate(no_empty=True)
    # Remember the results
    first_results = {file: dirinfo.get_relative(file) for file in files}
    # Resume populating, with different params to include the empty file
    dirinfo.populate(resume=True)
    for file in empty_files:
        assert dirinfo.get_relative(file) is not first_results[file], file
    for file in non_empty_files:
        assert dirinfo.get_relative(file) is first_results[file], file
    # Now delete a file, and make sure it disappears. Cover the cases where
    # the deleted file has and hasn't been hashed.
    missing_files = ('big', 'unequal')
    dirinfo.populate(no_empty=True, fast=True)
    for filename in missing_files:
        (subdir / filename).unlink()
    dirinfo.populate(resume=True, fast=True)
    for filename in missing_files:
        with pytest.raises(KeyError):
            dirinfo.get_relative(filename), filename

progress_tests = itertools.product([False, True], [False, True], [tqdm, DummyBar])

@pytest.mark.parametrize('no_empty, fast, progress', progress_tests)
def test_progress_bar(tmp_path, no_empty, fast, progress):
    """
    User can provide a progress bar factory, in particular tqdm.
    """
    file_size = 123
    subdir, files, dupe_groups = tempfiles(tmp_path, file_size)
    dirinfo = DirInfo(subdir)
    # We use a real tqdm here, because it's more important to test that the
    # integration with tqdm is correct, than UI details like the descriptions
    # being useful. We also test with DummyBar, to check the claim that it
    # demonstrates the required interface for user-defined progress.
    bars = []
    def tqdm_bar(*args, **kwargs):
        print(args, kwargs)
        bar = progress(*args, **kwargs)
        bars.append(bar)
        return bar
    dirinfo.populate(progress=tqdm_bar, no_empty=no_empty, fast=fast)
    # Make sure the progress bar was actually used: one progress bar for
    # reading the files, and one for hashing them.
    assert len(bars) == 2
    if progress is tqdm:
        # Progress bar shows all files, not just those hashed.
        assert bars[0].n == len(files)
    if progress is tqdm:
        # Upper bound on time is quite arbitrary and could be relaxed.
        assert 0 <= time.time() - bars[0].start_t <= 1
        assert 0 <= time.time() - bars[1].start_t <= 1
    check_everything(file_size, subdir, files, dupe_groups, dirinfo, no_empty, fast)

def test_fileinfo(tmp_path):
    """
    Test the dataclass that stores filename/hash data.
    """
    file_size = 123
    subdir, files, dupe_groups = tempfiles(tmp_path, file_size)
    dupes = set(itertools.chain.from_iterable(dupe_groups))
    dupe_sizes = set(len(files[name]) for name in dupes)
    dirinfo = DirInfo(subdir)
    dirinfo.populate(no_empty=True, fast=True)
    with_hash = without_hash = False
    for file in files:
        info = dirinfo.get_relative(file)
        if info.size in dupe_sizes:
            with_hash = True
            # Fields are tested by check_everything. Here we're interested in
            # the types.
            assert isinstance(info, FileStats) and isinstance(info, FileInfo)
        else:
            without_hash = True
            assert isinstance(info, FileStats) and not isinstance(info, FileInfo)
            # Test the addition of hash data to basic file stats.
            new_info = FileInfo.add_hash(info, b'hash')
            assert isinstance(new_info, FileInfo)
            assert new_info.hash == b'hash'
            now = datetime.now(tz=timezone.utc)
            assert now + timedelta(1) >= new_info.when >= now - timedelta(1)
            new_info = FileInfo.add_hash(info, b'hash', datetime(2000, 1, 1))
            assert isinstance(new_info, FileInfo)
            assert new_info.hash == b'hash'
            assert new_info.when == datetime(2000, 1, 1)
    assert with_hash
    assert without_hash

def test_serialisation(tmp_path):
    """
    Test failure modes. Success is tested in check_everything.
    """
    subdir = (tmp_path / 'sub')
    jsonfile = (tmp_path / 'sub.dirinfo.json')
    (subdir / 'dir').mkdir(parents=True)
    dirinfo = DirInfo(subdir)
    assert dirinfo.save() == os.fspath(jsonfile)
    # Not exactly a requirement, but for the tests to work we need this.
    assert jsonfile.exists()
    # If this fails, then testing that the bad cases fail is kind of pointless.
    assert DirInfo.load(subdir).base == os.fspath(subdir)
    # Make sure the encoder isn't accidentally used for something it can't handle.
    with pytest.raises(TypeError):
        json.dumps(object(), cls=Encoder)

    # Make sure bad json file contents are rejected
    def bad_jsonfile(jsondata):
        with open(jsonfile, 'w', encoding='utf8') as outfile:
            json.dump(jsondata, outfile)
        with pytest.raises(ValueError):
            DirInfo.load(subdir)
    bad_jsonfile({'foo': 'bar'})
    bad_jsonfile(None)

    # If the serialised base doesn't match the actual location, then something
    # is wrong and we should refuse to load it.
    assert dirinfo.save() == os.fspath(jsonfile)
    with open(jsonfile, 'r', encoding='utf8') as infile:
        jsondata = json.load(infile)
    jsondata['base'] += 'X'
    bad_jsonfile(jsondata)

    # If there's no data then load() fails, but cached() succeeds.
    jsonfile.unlink()
    with pytest.raises(FileNotFoundError):
        DirInfo.load(subdir)
    assert DirInfo.cached(subdir).base == subdir
