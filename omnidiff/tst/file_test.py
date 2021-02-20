
import binascii
import contextlib
import hashlib
import os
from pathlib import Path
import random
import sys
import time

import pytest

from omnidiff import file
from omnidiff.channel import Channel, QueueChannel

@pytest.mark.skipif(sys.platform != 'win32', reason='Windows only')
def test_longname_windows(tmp_path):
    r"""
    longname() is a helper to deal with long Windows paths. It returns a Path
    object which, when os.fspath() is called on it, absolutifies its location
    and adds the magic \\?\ prefix.
    """
    deep = ['1234567890'] * 50

    nonexists = Path(*deep, 'nonexists.txt')
    assert not os.fspath(nonexists).startswith('\\\\?\\')
    assert len(os.fspath(nonexists)) > 500
    assert os.fspath(file.longname(nonexists)).startswith('\\\\?\\')
    assert Path.cwd().drive not in os.fspath(nonexists)
    assert Path.cwd().drive in os.fspath(file.longname(nonexists))

    exists = tmp_path.joinpath(*deep, 'exists.txt')
    file.longname(exists).parent.mkdir(parents=True)
    file.longname(exists).touch()
    assert not os.fspath(exists).startswith('\\\\?\\')
    assert len(os.fspath(exists)) > 500
    assert os.fspath(file.longname(exists)).startswith('\\\\?\\')
    assert exists.drive in os.fspath(exists)
    assert exists.drive in os.fspath(file.longname(exists))

    prefixed = '\\\\?\\' + os.fspath(exists)
    assert os.fspath(prefixed).count('?') == 1
    assert os.fspath(file.longname(prefixed)).count('?') == 1

    prefixed = '\\\\?\\' + os.fspath(nonexists)
    assert os.fspath(prefixed).count('?') == 1
    assert os.fspath(file.longname(prefixed)).count('?') == 1

@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX only')
def test_longname_posix(tmp_path):
    """
    longname() is a helper to deal with long Windows paths. The POSIX version
    just does enough to allow for portable code.
    """
    deep = ['1234567890'] * 50

    nonexists = Path(*deep, 'nonexists.txt')
    assert not os.fspath(nonexists).startswith('\\\\?\\')
    assert len(os.fspath(nonexists)) > 500
    assert not os.fspath(file.longname(nonexists)).startswith('\\\\?\\')
    assert not os.fspath(nonexists).startswith('/')
    assert os.fspath(file.longname(nonexists)).startswith('/')

    exists = tmp_path.joinpath(*deep, 'exists.txt')
    file.longname(exists).parent.mkdir(parents=True)
    file.longname(exists).touch()
    assert not os.fspath(exists).startswith('\\\\?\\')
    assert len(os.fspath(exists)) > 500
    assert not os.fspath(file.longname(exists)).startswith('\\\\?\\')
    assert os.fspath(exists).startswith('/')
    assert os.fspath(file.longname(exists)).startswith('/')

def test_longname_portable(tmp_path):
    deep = ['1234567890'] * 50

    nonexists = Path(*deep, 'nonexists.txt')
    assert not os.fspath(nonexists).startswith('\\\\?\\')
    assert len(os.fspath(nonexists)) > 500
    assert not os.fspath(nonexists).startswith(os.path.sep)
    assert os.fspath(file.longname(nonexists)).startswith(os.path.sep)

    exists = tmp_path.joinpath(*deep, 'exists.txt')
    file.longname(exists).parent.mkdir(parents=True)
    file.longname(exists).touch()
    assert not os.fspath(exists).startswith('\\\\?\\')
    assert len(os.fspath(exists)) > 500
    assert os.fspath(file.longname(exists)).startswith(os.path.sep)

@contextlib.contextmanager
def tempfiles(tmpdir, file_size=0):
    (tmpdir / 'sub' / 'dir').mkdir(parents=True)
    (tmpdir / '.hidden').write_bytes(b'x' * file_size)
    deepfile = file.longname(Path(tmpdir, *(['1234567890'] * 50), 'deep'))
    deepfile.parent.mkdir(parents=True)
    deepfile.write_bytes(b'x' * file_size)
    filename = tmpdir / 'sub' / 'dir' / 'unhidden'
    filename.write_bytes(b'x' * file_size)
    yield filename

def test_recurse_files(tmp_path):
    """
    recurse_files() reads the files in a directory and its subdirectories.
    """
    with tempfiles(tmp_path) as path:
        chnl = file.recurse_files(path.parent)
        assert isinstance(chnl, Channel)
        files = set(chnl)
        assert path in files
        # Starting point is not included.
        assert path.parent not in files
        assert set(file.recurse_files(path)) == set()
        all_files = set(file.recurse_files(tmp_path))
        # Test that it really is just files, not directories
        assert path.parent not in all_files
        assert all(f.is_file() for f in all_files if f.name != 'deep')
        assert all(file.longname(f).is_file() for f in all_files)
        # Test that filenames starting . are included.
        assert tmp_path.joinpath('.hidden') in all_files
        # Test that long paths are traversed
        assert any(f.name == 'deep' for f in all_files), all_files  # pragma: no branch
        # A containment check without set() could lead to abandoning the
        # iterator. We'd need more files to confirm that really has caused an
        # early exit in the worker thread, and anyway the Channel tests look into
        # that, so let's just make sure the proper code works.
        with file.recurse_files(tmp_path).cancel_context() as chnl:
            assert path in chnl

def test_recurse_filestats(tmp_path):
    """
    recurse_filestats() is like recurse_files(), but returns more data than
    just the path of each file it finds.
    """
    with tempfiles(tmp_path, file_size=27):
        chnl = file.recurse_filestats(tmp_path)
        assert isinstance(chnl, Channel)
        all_files = set(chnl)
        # Check the same inclusion/exclusion as recurse_files()
        assert set(file.fullpath for file in all_files) == set(file.recurse_files(tmp_path))
        nonempty = [file for file in all_files if file.fullpath == tmp_path / '.hidden']
        assert len(nonempty) == 1
        nonempty = nonempty[0]
        # Check we have all the stats we want
        assert nonempty.fullpath == tmp_path / '.hidden'
        assert nonempty.basepath == tmp_path
        assert nonempty.relpath == Path('.hidden')
        assert nonempty.size == 27
        assert all(f.size == 27 for f in all_files), [(f, f.size) for f in all_files if f.size != 27]

def test_string_param(tmp_path):
    """
    recurse_files() and recurse_filestats() accept a string argument.
    """
    with tempfiles(tmp_path):
        assert set(file.recurse_files(tmp_path)) == set(file.recurse_files(str(tmp_path)))
        assert set(file.recurse_filestats(tmp_path)) == set(file.recurse_filestats(str(tmp_path)))

def test_channel_param(tmp_path):
    """
    recurse_files() and recurse_filestats() accept a channel parameter.
    """
    with tempfiles(tmp_path):
        assert set(file.recurse_files(tmp_path)) == set(file.recurse_files(tmp_path, channel=QueueChannel()))
        assert set(file.recurse_filestats(tmp_path)) == set(file.recurse_filestats(tmp_path, channel=QueueChannel()))

def test_threading(func=file.recurse_filestats):
    """
    recurse_filestats() really does use another thread to read.
    """
    # This test relies on you running on a drive with enough files that they
    # don't all get read within one time delta.
    delta = 0.01
    root = Path.cwd().anchor
    with func(root).cancel_context() as chnl:
        filecount = chnl.queue.qsize()
        while filecount == 0:
            time.sleep(delta)
            filecount = chnl.queue.qsize()
        for _ in range(100):
            if chnl.queue.qsize() > filecount:
                # Passed -- data is being written to the channel even
                # though we aren't calling any functions on it.
                break
            time.sleep(delta)
        else:
            pytest.fail(f'filecount in {root} stuck at {filecount}')

def test_threading_files():
    """
    recurse_files() really does use another thread to read.
    """
    test_threading(file.recurse_files)

@pytest.mark.inaccessible
def test_inaccessible():  # pragma: no cover
    """
    Bugfix regression test: must cope with inaccessible files.
    """
    # TODO - can we manufacture something?
    bad = Path.home().parent
    with file.recurse_filestats(bad).cancel_context() as chnl:
        for path in chnl:
            if path.size is None:
                with pytest.raises(OSError):
                    path.fullpath.is_file()
                break
        else:
            pytest.fail('Test not valid -- did not find any inaccessible files')

@pytest.fixture(scope='module')
def tmp_hashfiles(request, tmp_path_factory):
    # Helper to create a bunch of files to hash
    count = 20
    hashfunc = hashlib.sha256
    path = tmp_path_factory.mktemp(__name__, numbered=True)
    rand = random.Random('test1234')
    for blocks in range(0, count):
        data = bytes(rand.randint(0, 255) for _ in range(1000)) * (5 * blocks)
        hash = hashfunc(data).hexdigest()
        (path / hash).write_bytes(data)
    return path

@pytest.mark.parametrize('count', [1, 5, 100])
def test_hasher(count, tmp_hashfiles):
    """
    A hasher is a crew of threads that computes the hashes of the contents
    of the files we pass to it.
    """
    requests, responses = file.hasher(count)
    filenames = tuple(file.recurse_files(tmp_hashfiles))
    assert len(filenames) == 20
    requests.put_many(filenames).end()
    results = tuple(responses)
    assert len(results) == len(filenames)
    assert set(result[0] for result in results) == set(filenames)
    for filename, digest in results:
        assert binascii.hexlify(digest).decode('ascii') == filename.name

def test_hash_long_path(tmp_path):
    """
    Bugfix regression test: must cope with long file paths.
    """
    dir = '0123456789' * 2
    tmp_file = tmp_path.joinpath(dir)
    while len(os.fspath(tmp_file)) <= 512:
        tmp_file = tmp_file.joinpath(dir)
    file.longname(tmp_file).parent.mkdir(parents=True)
    file.longname(tmp_file).write_bytes(b'123')
    expected = hashlib.sha256(b'123').digest()
    requests, responses = file.hasher(1)
    requests.put_many([tmp_file]).end()
    assert next(iter(responses)) == (tmp_file, expected)

def test_hash_order(tmp_path):
    """
    Files should be hashed largest first. This is basically an optimisation
    (to prevent the slowest zebra being a massive file). However, it's also a
    nice usability feature, so the progress bar consistently speeds up rather
    than speeding and halting. Sure, it would be even better if the progress
    bar was constant speed, but that's a harder feature request.
    """
    # Choose sizes to give us a chance of adding them to the channel faster
    # than the other end processes them.
    base_size = 10 * 1024
    increment_size = 10 * 128
    filenames = tuple((tmp_path / filename) for filename in '123456789')
    for idx, path in enumerate(filenames, 1):
        path.write_bytes(b'x' * (base_size + (idx * increment_size)))
    requests, responses = file.hasher(1)
    requests.put_many(filenames).end()
    order_returned = [os.path.basename(p[0]) for p in responses]
    # So, the question is how close to ordered we expect the results to be.
    # Generally speaking, we have to allow the worker to take files off the
    # queue before we've written everything, but the rest should appear in
    # decreasing order. Beware, though, that this test is not fully
    # deterministic, and could fail for no good reason.
    idx = order_returned.index('9')
    results = order_returned[idx:]
    assert len(results) == len(filenames) - idx
    # It's not a good test if we're only looking at a few files.
    assert len(results) >= 5
    # Rely on the fact that file sizes are in the same order as the names!
    assert sorted(results, reverse=True) == results
