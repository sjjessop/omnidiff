
from __future__ import annotations

import abc
import contextlib
from dataclasses import dataclass, field
from enum import Enum
import logging
import queue
import threading
from typing import Any, Callable, Iterable, Iterator, Tuple, Union

logger = logging.getLogger(__name__)

class State(Enum):
    """
    Enumeration for the current state of a :obj:`Channel`: ``OPEN``,
    ``FINISHED``, or ``CANCELLED``.
    """
    OPEN = 'OPEN'
    FINISHED = 'FINISHED'
    CANCELLED = 'CANCELLED'

class Channel(abc.ABC):
    """
    Abstract class representing pipe-like communications.

    A channel consists of a queue of objects, plus the ability for both readers
    and writers to signal the end of the communication.

    At the writer end of a channel, the principal operations are :func:`put` to
    write an object to the channel, :func:`end` to indicate that no more
    objects will be written, and :func:`cancel` to indicate that the channel
    should no longer be used. The difference between :func:`end` and
    :func:`cancel` is that after the former, readers will continue reading all
    objects from the channel, whereas after the latter they will stop reading
    as soon as possible.

    At the reader end of a channel, the principal operations are :func:`get` to
    read an object from the channel and :func:`cancel` to indicate that the
    channel should no longer be used. The Channel object is also iterable,
    yielding each object from the channel as soon as it is available.

    This interface is suitable for communication between threads or "bigger"
    (communicating between processes or machines fits the interface too,
    although in that case of course the different ends of the communication
    cannot share a single object representing the channel). The interface is
    not suitable for communication between async tasks, or in general for async
    use, since it blocks when there is nothing to read.
    """
    def __init__(self):
        super().__init__()
        self.state = State.OPEN
    @abc.abstractmethod
    def put(self, value: Any) -> None:
        """
        Write `value` to the channel.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called.
        """
        raise NotImplementedError
    def put_many(self, iterable: Iterable) -> Channel:
        """
        Convenience function. Write all values from `iterable` to the channel.

        :return: `self`. It is very common to need to call :func:`end()` here.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called.
        """
        for value in iterable:
            self.put(value)
        return self
    @abc.abstractmethod
    def get(self) -> None:
        """
        Read one object from the channel.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called and all objects
          have been read.
        """
        raise NotImplementedError
    def __iter__(self):
        with contextlib.suppress(ChannelError):
            while True:
                yield self.get()
    @abc.abstractmethod
    def end(self) -> None:
        """
        Indicate no more objects will be written.
        """
        raise NotImplementedError
    @abc.abstractmethod
    def cancel(self) -> None:
        """
        Indicate no more reading or writing is allowed. This is not guaranteed
        to take place immediately, especially in implementations where readers
        and writers can be remote.
        """
        raise NotImplementedError
    @contextlib.contextmanager
    def cancel_context(self) -> Iterator[Channel]:
        """
        Convenience function. Context manager, for when you are responsible
        for cancelling a Channel.
        """
        try:
            yield self
        finally:
            self.cancel()
    def check(self, include_finished: bool = True) -> None:
        """
        Check whether the channel is still open for writing and/or reading.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called and
          `include_finished` is true.
        """
        if self.state == State.CANCELLED:
            raise Cancelled()
        if include_finished and self.state == State.FINISHED:
            raise Finished()

class _BigSentinel(object):
    """
    A class that compares bigger than anything the user writes as a value.
    Therefore it will be read last in the case the channel is finished.
    """
    def __lt__(self, other):
        return False
    def __gt__(self, other):
        return not isinstance(other, _BigSentinel)

class QueueChannel(Channel):
    """
    Concrete class representing a :obj:`Channel` based on :obj:`queue`.

    The channel is thread-safe, meaning that multiple readers and writers can
    use it simultaneously from different threads. Each object written will be
    read by only one reader.

    :param priority: if False, the channel uses a FIFO :obj:`~queue.Queue`,
      meaning that objects are read in the same order they were written. If
      True, the channel uses a :obj:`~queue.PriorityQueue`, meaning that each
      time an object is read, the smallest object available is returned
      (objects written to the channel) therefore must be comparable. If
      callable, the channel uses a :obj:`~queue.PriorityQueue`, and the
      comparisons are peformed on the results of calling ``priority`` on each
      object, instead of on the objects themselves.
    """
    sentinel = _BigSentinel()
    queue: queue.Queue
    def __init__(self, priority: Union[bool, Callable] = False):
        super().__init__()
        if callable(priority):
            self.queue = KeyedPriorityQueue(priority)
        elif priority:
            self.queue = queue.PriorityQueue()
        else:
            self.queue = queue.Queue()
        self.lock = threading.Lock()
    def put(self, value: Any) -> None:
        """
        Write ``value`` to the channel. Never blocks.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called.
        """
        self.check()
        self.queue.put(value)
    def get(self) -> Any:
        """
        Read one object from the channel. If there is nothing to read, then
        blocks until something is available to read.

        :raise: :obj:`Cancelled` if :func:`cancel` has been called.
        :raise: :obj:`Finished` if :func:`end` has been called and all objects
          have been read.
        """
        self.check(False)
        result = self.queue.get()
        if result is self.sentinel:
            # Just in case there are more readers waiting.
            self.queue.put(result)
            self.check()
        return result
    def end(self) -> None:
        self._set_state(State.FINISHED, old=State.OPEN)
    def cancel(self) -> None:
        self._set_state(State.CANCELLED)
    def _set_state(self, new: State, *, old: State = None) -> None:
        with self.lock:
            if (old is None and self.state != new) or self.state == old:
                self.state = new
                # Write something in order to wake up anyone waiting.
                self.queue.put(self.sentinel)

class ChannelError(Exception):
    """
    Base class for exceptions relating to channels.
    """
    @classmethod
    @contextlib.contextmanager
    def suppress(cls) -> Iterator[None]:
        """
        Like contextlib.suppress for whichever exception class you call this
        function on, except that (unlike contextlib.suppress) you can use it
        as a decorator. The decorated function returns None if the exception
        occurs.
        """
        with contextlib.suppress(cls):
            yield

class Finished(ChannelError):
    """
    Exception raised after :func:`~Channel.end` has been called. If reading
    then it is only raised once all objects have been read from the channel. If
    writing then it is raised on any attempt to write after calling
    :func:`~Channel.end`.
    """

class Cancelled(ChannelError):
    """
    Exception raised when reading or writing after :func:`~Channel.cancel` has
    been called.
    """

class WrappedChannel(Channel):
    """
    WrappedChannel is a channel which uses some other channel instead of having
    its own queue. You subclass this if you want to interfere with the values
    read to or written from the channel.

    A new method is added, close(), which is called by both cancel() and end().
    You can use it to release any put()-side resources that won't be needed by
    get(), so that they don't hang around until all data has been read and the
    channel is eventually destroyed.
    """
    other: Channel
    def __init__(self, other: Channel):
        super().__init__()
        self.other = other
        # Catch implementation errors if anything access self.state instead of
        # other.state.
        del self.state
    def check(self, *args, **kwargs) -> None:
        return self.other.check(*args, **kwargs)
    def put(self, value: Any) -> None:
        self.other.put(value)
    def get(self) -> Any:
        return self.other.get()
    def end(self) -> None:
        self.other.end()
        self.close()
    def cancel(self) -> None:
        self.other.cancel()
        self.close()
    def close(self) -> None:
        """
        Free resources that are no longer needed once :func:`~Channel.end` or
        :func:`~Channel.cancel` has been called. The base class implementation
        does nothing.
        """

@dataclass(frozen=True, order=True)
class _PrioritizedItem:
    priority: Any
    item: Any = field(compare=False)

class KeyedPriorityQueue(queue.PriorityQueue):
    """
    Like a :obj:`~queue.PriorityQueue`, except that objects are compared
    according to the specified key function.

    :param get_key: Function which will be passed each object written to the
      queue, and must return the sort key for that object.
    """
    def __init__(self, get_key: Callable):
        super().__init__()
        self.get_key = get_key
    def get(self, *args, **kwargs) -> Any:
        """Read one object."""
        return super().get(*args, **kwargs).item
    def put(self, item: Any, *args: Any, **kwargs: Any) -> None:
        """Write one object."""
        if item is QueueChannel.sentinel:
            key = item
        else:
            key = self.get_key(item)
        super().put(_PrioritizedItem(priority=key, item=item), *args, **kwargs)

def thread_source(
    iterable: Iterable,
    *,
    daemon: bool = False,
    channel: Channel = None,
) -> Channel:
    """
    Enumerate iterable in a new thread, writing each item to a Channel.

    Beware that Python cannot exit until all non-daemon threads have exited.
    If the reader might want to bail out before the iterable (and hence the
    thread) have completed, then two measures are available. You can pass
    ``daemon=True`` to run the thread as a daemon (so, it will be interrupted
    if necessary at exit), or the reader can call :func:`cancel` on the
    channel. Using :func:`~Channel.cancel_context` will ensure that the reader
    always calls :func:`~Channel.cancel`.

    :param daemon: whether to create the thread as daemon.
    :param channel: the Channel to write to, or None to create a new one.
    :return: the Channel.
    """
    if channel is None:
        channel = QueueChannel()
    @Cancelled.suppress()
    def target():
        try:
            channel.put_many(iterable)
        except Cancelled:
            pass
        except BaseException:
            logger.exception('thread_source worker exception')
            channel.cancel()
        else:
            channel.end()
    threading.Thread(target=target, daemon=daemon).start()
    return channel

def thread_crew(
    count: int,
    worker: Callable,
    *,
    mode: str = '1:1',
    requests: Channel = None,
    responses: Channel = None,
) -> Tuple[Channel, Channel]:
    """
    Create a crew of worker threads, each of which is reading from the Channel
    returned as "requests", and writing to the Channel returned as "responses".

    A crew with `count` 1 is in effect a single-threaded server responding to
    requests. A higher count can process requests in parallel, although as
    ever in Python, bear in mind the limitations of the GIL.

    The worker threads shut down once the requests Channel is either cancelled
    or else is finished and all its values have been read and responded to.
    They  are also shut down if the responses Channel is cancelled.

    `worker` is the function executed in each worker thread. The signature
    depends on the mode:

    '1:1' (one-to-one): The worker function must accept
    (value, check_cancelled), where value has been read from the requests
    channel. The return value is written to the responses Channel.
    ``check_cancelled()`` can be called with no args and raises Cancelled if
    either channel has been cancelled. Slow worker functions can use this to
    avoid fully processing a request, in the case where the crew has been
    cancelled.

    '1:m' (one-to-many): The worker function must accept
    (value, check_cancelled, out_channel). The first two parameters are as for
    1:1 mode, and the last is the responses channel. The return value is
    ignored.

    'm:m' (many-to-many): The worker function must accept
    (in_channel, out_channel). The second parameter is as for 1:m mode, and the
    first is the requests channel. Unlike the other two modes, in m:m mode the
    worker function is responsible for looping, until calling
    :func:`~Channel.get()` on `in_channel` raises a :obj:`ChannelError`. The
    return value is ignored.

    :param count: Number of worker threads.
    :param worker: Function called in the worker threads.
    :param mode: One of ``'1:1'``, ``'1:m'``, ``'m:m'``. Controls how the
      `worker` function is called.
    :param requests: Channel from which the workers read objects, or None to
      create a new Channel.
    :param responses: Channel to which the workers write objects, or None to
      create a new Channel.
    :return: (requests, responses)
    """
    if requests is None:
        requests = QueueChannel()
    if responses is None:
        responses = QueueChannel()
    def check():
        requests.check(False)
        responses.check(False)
    # The "inner core" of the thread target depends on the mode
    if mode == '1:1':
        def inner_target():
            while True:
                responses.put(worker(requests.get(), check))
    elif mode == '1:m':
        def inner_target():
            while True:
                worker(requests.get(), check, responses)
    elif mode == 'm:m':
        def inner_target():
            worker(requests, responses)
    else:
        raise ValueError(mode)
    # Around this we wrap some stuff to end/cancel the channels on exit,
    # and log any unexpected exceptions.
    threads_finished = 0
    lock = threading.Lock()
    @Cancelled.suppress()
    def target():
        nonlocal threads_finished
        try:
            inner_target()
        except Cancelled:
            requests.cancel()
            responses.cancel()
        except Finished:
            pass
        except BaseException:
            logger.exception('thread crew worker exception')
            requests.cancel()
            responses.cancel()
        finally:
            # Last thread out needs to end the responses thread.
            with lock:
                threads_finished += 1
                if threads_finished == count:
                    responses.end()
    for idx in range(count):
        threading.Thread(target=target).start()
    return requests, responses

if 0:
    thread_crew(1, lambda value: 1, mode=3)
