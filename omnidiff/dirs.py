
from __future__ import annotations

import binascii
from collections import defaultdict
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
import pathlib
from typing import (
    Any, Callable, Dict, FrozenSet, Iterable, Iterator, Optional, Set,
)

from omnidiff import channel, file
from omnidiff.file import PathLike

@dataclass(frozen=True, order=True)
class FileInfo(file.FileStats):
    """
    Represents a file and its hash.
    """
    hash: bytes
    when: datetime
    @classmethod
    def add_hash(cls, old: file.FileStats, hash: bytes, when: datetime = None) -> FileInfo:
        if when is None:
            when = datetime.now(tz=timezone.utc)
        return cls(
            _base_str=old._base_str,
            _rel_str=old._rel_str,
            size=old.size,
            hash=hash,
            when=when,
        )

# Although this is frozen, beware the hidden members are mutable and so the
# object has state.
@dataclass(frozen=True)
class DirInfo:
    """
    Object representing the location and contents of a directory. Call
    :func:`populate` to scan the contents.

    :param base: The directory location.
    """
    base: PathLike
    # All files found, indexed by relative path. This contains either a
    # FileStats or a FileInfo object, according to whether we have hashed the
    # file (yet).
    _files_by_rel_str: Dict[str, file.FileStats] = field(default_factory=dict)
    # All files hashed, indexed by size and hash. This always contains the
    # hydrated FileInfo.
    _files_by_hash: Dict[Optional[int], Dict[bytes, Set[FileInfo]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    def populate(
        self,
        *,
        no_empty: bool = False,
        fast: bool = False,
        resume: bool = False,
        threads: int = 1,
        progress: Callable = None,
    ) -> None:
        """
        Recursively search the directory, recording file names and hashes. By
        default all files are hashed, but for speed you can opt to only hash
        files under certain conditions.

        :param no_empty: Do not hash files of size 0.
        :param fast: Only hash files if their size matches that of at least one
          other file somewhere under this directory. This is enough hashes to
          detect any duplicate files contained under the directory.
        :param resume: Re-use any existing results (default is to restart from
          scratch).
        :param threads: Number of threads to use for hashing.
        :param progress: Factory to create a progress bar. This is designed to
          be compatible with `tqdm.tqdm <https://tqdm.github.io/>`_, but you
          can substitute anything with a matching interface. :obj:`DummyBar`
          is a minimal implementation of the required interface, so if you want
          to write your own then start with the `DummyBar` source.
        """
        if not resume:
            # Start from scratch.
            self._files_by_rel_str.clear()
            self._files_by_hash.clear()
        to_hash = 0
        # This set is populated by existing_hash(), which is only called if
        # resume is True, because it's only needed if resume is True but it
        # could get quite large.
        seen = set()

        def existing_hash(fstats: file.FileStats) -> bool:
            seen.add(fstats._rel_str)
            with contextlib.suppress(KeyError):
                oldstats = self.get_relative(fstats._rel_str)
                if oldstats.size == fstats.size and hasattr(oldstats, 'hash'):
                    return True
            return False
        class Store(channel.WrappedChannel):
            def put(inner_self, fstats: file.FileStats) -> None:
                if not resume or not existing_hash(fstats):
                    self._store_file(fstats)
                # Pass on even if we didn't store, because the SizeFilter needs
                # to see it, and it's nice to include it in the progress too.
                super().put(fstats)
        class Progress(channel.WrappedChannel):
            def __init__(inner_self, other):
                super().__init__(other)
                inner_self.bar = progress(desc='Finding files')
            def close(inner_self):
                inner_self.bar.set_description_str(desc='Found files')
                inner_self.bar.close()
            def put(inner_self, fstats: file.FileStats) -> None:
                super().put(fstats)
                inner_self.bar.update(1)
        class SkipEmpty(channel.WrappedChannel):
            def put(inner_self, fstats: file.FileStats) -> None:
                if fstats.size != 0:
                    super().put(fstats)
                else:
                    super().check()
        class SizeFilter(channel.WrappedChannel):
            many = object()
            def __init__(inner_self, other):
                super().__init__(other)
                inner_self.size_groups = {}
            def close(inner_self):
                super().close()
                del inner_self.size_groups
            def put(inner_self, fstats: file.FileStats) -> None:
                previous = inner_self.size_groups.get(fstats.size)
                if previous is None:
                    # First file of this size. Don't hash, just remember it.
                    super().check()
                    inner_self.size_groups[fstats.size] = fstats
                elif previous is inner_self.many:
                    # Just hash it.
                    super().put(fstats)
                else:
                    # We've seen exactly one file of this size before.
                    super().put(previous)
                    super().put(fstats)
                    # 2 or more files is all the same to us.
                    inner_self.size_groups[fstats.size] = inner_self.many
        class SkipExisting(channel.WrappedChannel):
            def put(inner_self, fstats: file.FileStats) -> None:
                if existing_hash(fstats):
                    # We already hashed it
                    super().check()
                else:
                    super().put(fstats)
        class Filename(channel.WrappedChannel):
            def put(inner_self, fstats: file.FileStats) -> None:
                nonlocal to_hash
                super().put(fstats.fullpath)
                to_hash += 1
        wrappers = [
            Store,
            Progress if progress is not None else None,
            SkipEmpty if no_empty else None,
            SizeFilter if fast else None,
            SkipExisting if resume else None,
            Filename,
        ]
        requests, responses = file.hasher(threads)
        with requests.cancel_context():
            filtered = requests
            for wrapper in reversed(wrappers):
                if wrapper is not None:
                    filtered = wrapper(filtered)
            file.recurse_filestats(self.base, channel=filtered)
            if progress is None:
                progress = DummyBar
            with progress(responses, desc='Hashing files', total=to_hash) as bar:
                for path, hash in bar:
                    self._record_hash(path, hash)
                    # Update the count, since we're concurrently finding files.
                    bar.total = to_hash
        if resume:
            # We might still have records hanging around for files that no
            # longer exist. It's important we compute the set of them before we
            # start modifying the dictionary.
            for filename in (self._files_by_rel_str.keys() - seen):
                stats = self._files_by_rel_str.pop(filename)
                if isinstance(stats, FileInfo):
                    self._files_by_hash[stats.size][stats.hash].remove(stats)
    def _store_file(self, stats: file.FileStats) -> None:
        # We use a private attribute of FileStats here in order to save memory
        # by sharing the same string instance.
        self._files_by_rel_str[stats._rel_str] = stats
    def _store_hash(self, stats: FileInfo):
        self._files_by_hash[stats.size][stats.hash].add(stats)
        return stats
    def _record_hash(self, path: pathlib.Path, hash: bytes, when: datetime = None):
        relative = os.fspath(path.relative_to(self.base))
        old_stats = self._files_by_rel_str[relative]
        # TODO: maybe could intern the hash to save memory?
        new_stats = FileInfo.add_hash(old_stats, hash=hash, when=when)
        self._store_file(new_stats)
        self._store_hash(new_stats)
    @property
    def file_count(self):
        """
        The number of files found.
        """
        return len(self._files_by_rel_str)
    def get_relative(self, rel: str) -> file.FileStats:
        """
        Return what we know about the specified file.

        If the file has been hashed, then the return will be a :obj:`FileInfo`.

        :param rel: Path to file, relative to this directory's location.
        :raises: :obj:`KeyError` if file not known.
        """
        return self._files_by_rel_str[rel]
    def dupe_groups(self) -> Iterator[FrozenSet[FileInfo]]:
        """
        Iterator. Each value yielded is a set of :obj:`FileInfo` objects,
        representing a group of files with the same hash. Only groups of two
        or more files are included.
        """
        for sizegroup in self._files_by_hash.values():
            for hashgroup in sizegroup.values():
                if len(hashgroup) > 1:
                    yield frozenset(hashgroup)
    def save(self) -> str:
        """
        Serialise the directory info. The data is stored alongside the
        directory, in a file named after the directory with '.dirinfo.json'
        appended.

        The absolute path of the directory is stored in the JSON, for safety,
        so if you later move the directory with its JSON file, then you may
        need to edit the JSON file (or just delete and rebuild it). An option
        or tool to relocate dirinfo files may be provided in future.
        """
        filename = os.fspath(self.base) + '.dirinfo.json'
        with open(filename, 'w', encoding='utf8') as outfile:
            json.dump(self, outfile, cls=Encoder, indent=1, ensure_ascii=False, sort_keys=True)
        return filename
    @classmethod
    def load(cls, dir: PathLike) -> DirInfo:
        """
        Load serialised info for the specified directory.

        :raises: :obj:`FileNotFoundError` if dirinfo not present.
        """
        filename = os.fspath(dir) + '.dirinfo.json'
        with open(filename, 'r', encoding='utf8') as infile:
            result = json.load(infile, object_hook=Encoder.object_hook)
            if not isinstance(result, cls):
                raise ValueError(f'Content of {dir!r} is wrong format')
            if os.fspath(result.base) != os.path.abspath(dir):
                raise ValueError(f'Base dir does not match: got {result.base!r}, expected {dir!r}')
            return result
    @classmethod
    def cached(cls, dir: PathLike) -> DirInfo:
        """
        Create a DirInfo object for the specified directory, using cached data
        if present or a brand new object if not.
        """
        try:
            return cls.load(dir)
        except FileNotFoundError:
            return cls(dir)

class DummyBar(object):
    """
    A progress bar that does nothing.

    This demonstrates the interface required by :func:`~DirInfo.populate`. All
    methods are the same as `tqdm <https://tqdm.github.io/docs/tqdm/#__init__>`_,
    except that unused parameters aren't listed here.
    """
    def __init__(self, iterable: Iterable = None, *, desc: str, total: int = 0) -> None:
        self._responses = iterable
        self.total = total
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return
    def __iter__(self) -> Iterator:
        return iter(self._responses or ())
    def set_description_str(self, desc: str) -> None:
        return
    def update(self, n: int = 1) -> None:
        return
    def close(self) -> None:
        return

class Encoder(json.JSONEncoder):
    """
    Class for encoding and decoding DirInfo objects as JSON.

    To save duplication, the shared base_str from each FileStats object is
    stored only once, and we handle serialising the other fields.
    """
    def default(self, o: Any) -> Any:
        """
        Encode object to a jsonable dictionary.
        """
        if isinstance(o, FileInfo):
            return {
                '_rel_str': o._rel_str,
                'size': o.size,
                'hash': binascii.hexlify(o.hash).decode('ascii'),
                'when': o.when.isoformat(),
            }
        if isinstance(o, file.FileStats):
            return {
                '_rel_str': o._rel_str,
                'size': o.size,
            }
        if isinstance(o, DirInfo):
            return {
                'base': os.path.abspath(o.base),
                'files': list(o._files_by_rel_str.values()),
            }
        return super().default(o)
    @staticmethod
    def object_hook(values: Dict[str, Any]) -> Any:
        """
        Modify dictionary for object. Reverse of :func:`default`.
        """
        if '_rel_str' in values:
            # Then we have a file object (either FilesStats or FileInfo).
            # We will convert it to an object later, once we have the base dir.
            # For now, just prepare the parameters.
            if 'hash' in values:
                values['hash'] = binascii.unhexlify(values['hash'].encode('ascii'))
                values['when'] = datetime.fromisoformat(values['when'])
            return values
        if 'base' in values:
            # Then we have a DirInfo object
            record = DirInfo(values['base'])
            for filestats in Encoder._iter_files(values['files'], record.base):
                record._store_file(filestats)
                if isinstance(filestats, FileInfo):
                    record._store_hash(filestats)
            return record
        # We don't accept any other objects or dictionaries.
        raise ValueError(repr(values)[0:100])
    @staticmethod
    def _iter_files(files_json, base_str) -> Iterable[file.FileStats]:
        for file_json in files_json:
            stats = file.FileStats(base_str, file_json['_rel_str'], file_json['size'])
            if 'hash' in file_json:
                stats = FileInfo.add_hash(stats, file_json['hash'], file_json['when'])
            yield stats
