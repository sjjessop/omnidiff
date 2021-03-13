"""
Functions to list and hash files, using threads and Channels for concurrency.

This module also handles long file paths on Windows. The trick is to always
call :func:`longname` on any path before attempting a file-based operation on
it. :func:`longname` exists on all platforms, so you can use it to write code
that won't mysteriously fail to read files when used on a deep Windows file
hierarchy.

All functions that take a file or directory name as a parameter, accept either
a string or :obj:`os.PathLike` object.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path, PosixPath, WindowsPath
from typing import Callable, Iterator, Optional, Tuple, Union

from .channel import Channel, QueueChannel, thread_crew, thread_source

#: Type alias for an object that can be passed to both :func:`os.fspath` and
#: :obj:`pathlib.Path` to respectively create a string or Path representation
#: of a file location. :obj:`os.PathLike` is not suitable to represent this,
#: since it does not admit :obj:`str`.
PathLike = Union[os.PathLike, str]

class LongWindowsPath(WindowsPath):
    r"""
    Behaves just like :obj:`~pathlib.WindowsPath` so far as filename
    manipulations and :func:`str` are concerned, but when passed to system
    calls it reports its location in a way that:

    * Works for long paths, hence starts with ``\\?\``
    * Works for relative paths, hence is absolute, since for example
      ``os.path.isdir('\\\\?\\foo')`` returns False if foo is a directory.
    """
    __slots__ = ()
    def __fspath__(self) -> str:
        result = os.path.abspath(super().__fspath__())
        # r-strings cannot end with a backslash
        longprefix = '\\\\?\\'
        if result.startswith(longprefix):
            return result
        return longprefix + result

class LongPosixPath(PosixPath):
    """
    For consistency with :obj:`LongWindowsPath`, this class behaves just like
    :obj:`~pathlib.PosixPath`, but when passed to system calls reports its
    location as an absolute path.

    This means if you write your code on Windows and accidentally rely on the
    fact that Windows long paths are always passed as absolute, then your long
    paths will still be passed as absolute on other platforms.
    """
    __slots__ = ()
    def __fspath__(self) -> str:
        return os.path.abspath(super().__fspath__())

longname = LongWindowsPath if os.name == 'nt' else LongPosixPath

def _walk(dir: PathLike) -> Iterator[Path]:
    # You might think we can use pathlib.Path.rglob here, since it returns all
    # files despite being called "glob" (https://bugs.python.org/issue26096).
    # But then to distinguish files from directories we need to call
    # .is_file(), which uses os.stat(). That can fail for some files where
    # os.walk() successfully distinguishes.
    longdir = os.fspath(longname(dir))
    dir = Path(dir)
    for path, dirs, files in os.walk(longdir):
        shortpath = dir.joinpath(Path(path).relative_to(longdir))
        for file in files:
            yield Path(shortpath, file)

def recurse_files(dir: PathLike, *, channel: Channel = None) -> Channel:
    """
    Starts a worker thread that finds all files in `dir` (recursively) and
    writes them to a :obj:`Channel`. Each file found is written as a
    :obj:`pathlib.Path` object.

    :param dir: The directory to search.
    :param channel: Channel to write to, or None to create a new one.
    :return: The Channel.
    """
    return thread_source(_walk(dir), channel=channel)

@dataclass(frozen=True)
class FileStats(object):
    """
    Represents a file, in the context of a base directory. Usually you will
    have a number of :obj:`FileStats` objects with a common base.
    """
    __slots__ = ('_base_str', '_rel_str', 'size')
    # Store as stings to save space.
    _base_str: str
    _rel_str: str
    size: Union[int, None]
    @classmethod
    def relative_to(cls, base: PathLike) -> Callable[[PathLike], FileStats]:
        """
        Return a function which maps a file path to a :obj:`FileStats` object
        for that file, in the context of the specified base. So, you can call
        :func:`relative_to` once, and use that to create many files with the
        same base.
        """
        base_str = os.fspath(base)
        def builder(path: PathLike) -> FileStats:
            path = Path(path)
            try:
                size: Optional[int] = longname(path).stat().st_size
            except OSError:  # pragma: no cover
                size = None
            return cls(base_str, os.fspath(path.relative_to(base)), size)
        return builder
    @property
    def fullpath(self) -> Path:
        """
        The combined base directory and relative path of the file, as a
        :obj:`~pathlib.Path`. Note that if the base is a relative path, then
        this "full" path is still a relative path, not an absolute one.
        """
        return Path(self._base_str, self._rel_str)
    @property
    def basepath(self) -> Path:
        """
        The base directory, as a :obj:`~pathlib.Path`.
        """
        return Path(self._base_str)
    @property
    def relpath(self) -> Path:
        """
        The relative path from the base directory to the file, as a
        :obj:`~pathlib.Path`.
        """
        return Path(self._rel_str)

def recurse_filestats(dir: PathLike, *, channel: Channel = None) -> Channel:
    """
    Starts a worker thread that finds all files in `dir` (recursively) and
    writes them to a :obj:`Channel`. Each file found is written as a
    :obj:`FileStats` object.

    :param dir: The directory to search.
    :param channel: Channel to write to, or None to create a new one.
    :return: The Channel.
    """
    file_iterator = map(FileStats.relative_to(dir), _walk(dir))
    return thread_source(file_iterator, channel=channel)

def hasher(
    count: int,
    *,
    hashfunc: Callable = hashlib.sha256,
) -> Tuple[Channel, Channel]:
    """
    Return a pair of Channels backed by a crew of threads. You write filenames
    to the first Channel, and get back pairs of (filename, hashdigest) from the
    second Channel.

    :param count: Number of worker threads to use to hash files. File
      reading and hashing can occur concurrently so far as Python is concerned
      (since the GIL is released), but the effect of larger numbers of threads
      depends on the file storage. I have used internal SSDs and external
      magnetic drives, and found that the SSDs benefit from multiple threads,
      but the spinny-disks tend to be slower.
    :param hashfunc: A constructor for hash objects, like obj:`hashlib.sha256`.
    :return: A tuple containing the request Channel and the response Channel.
    """
    def worker(filename: PathLike, check: Callable):
        with longname(filename).open('rb') as infile:
            hash = hashfunc()
            for chunk in iter(lambda: infile.read(65536), b''):
                check()
                # hashlib releases the GIL for large chunks of data, so we get
                # a benefit from threading here as well as in read().
                hash.update(chunk)
        return filename, hash.digest()
    def biggest(filename: PathLike) -> int:
        try:
            return longname(filename).stat().st_size * -1
        except OSError:  # pragma: no cover
            return 0
    return thread_crew(count, worker, requests=QueueChannel(priority=biggest))
