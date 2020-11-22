
import itertools
import multiprocessing
import threading
import time

import pytest

from omnidiff import channel
from omnidiff.channel import QueueChannel as Channel

def test_put_get():
    """
    Channel is a FIFO queue. Write with put() or put_many(), then read with
    get().
    """
    chnl = Channel()
    chnl.put(1)
    chnl.put(2)
    assert chnl.get() == 1
    chnl.put(3)
    assert chnl.get() == 2
    assert chnl.get() == 3
    chnl.put_many([4, 5])
    assert chnl.get() == 4
    assert chnl.get() == 5

def test_iterate():
    """
    You can read a Channel to the end by iterating it.
    """
    chnl = Channel()
    chnl.put_many(range(5))
    chnl.end()
    assert list(chnl) == [0, 1, 2, 3, 4]

def test_finished():
    """
    Once you end a channel, get() raises Finished.
    """
    chnl = Channel()
    chnl.end()
    with pytest.raises(channel.Finished):
        chnl.get()
    with pytest.raises(channel.Finished):
        chnl.get()
    chnl = Channel()
    chnl.put_many([1, 2, 3]).end()
    assert chnl.get() == 1
    assert chnl.get() == 2
    assert chnl.get() == 3
    with pytest.raises(channel.Finished):
        chnl.get()
    with pytest.raises(channel.Finished):
        chnl.get()

def test_cancelled():
    """
    Once you cancel a Channel, get() raises Cancelled.
    """
    chnl = Channel()
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        chnl.get()
    with pytest.raises(channel.Cancelled):
        chnl.get()

def test_priority():
    """
    FIFO behaviour can be changed to a priority queue.
    """
    chnl = Channel(priority=True)
    values = (3, 2, 1, 1, 2, 3, 7)
    chnl.put_many(values).end()
    assert tuple(chnl) == tuple(sorted(values))
    # And this still works with the gets mixed into the puts
    chnl = Channel(priority=True)
    chnl.put(2)
    chnl.put(1)
    assert chnl.get() == 1
    chnl.put(0)
    assert chnl.get() == 0
    chnl.put(3)
    assert chnl.get() == 2
    assert chnl.get() == 3

def test_priority_callable():
    """
    Prioritised channel can have custom sort order.
    """
    chnl = Channel(priority=lambda value: -value)
    values = (3, 2, 1, 1, 2, 3, 7)
    chnl.put_many(values).end()
    assert tuple(chnl) == tuple(sorted(values, reverse=True))
    # And this still works with the gets mixed into the puts
    chnl = Channel(priority=lambda value: -value)
    chnl.put(2)
    chnl.put(1)
    assert chnl.get() == 2
    chnl.put(0)
    assert chnl.get() == 1
    chnl.put(3)
    assert chnl.get() == 3
    assert chnl.get() == 0

def test_cancel_context():
    """
    cancel_context() auto-cancels channels on exit.
    """
    with Channel().cancel_context() as chnl:
        chnl.put_many([1, 2])
        assert chnl.get() == 1
    with pytest.raises(channel.Cancelled):
        chnl.get()

def test_exception_base():
    """
    The Channel exceptions have a common base class.
    """
    assert issubclass(channel.Finished, channel.ChannelError)
    assert issubclass(channel.Cancelled, channel.ChannelError)

def test_suppressed():
    """
    The Channel exceptions both have a special suppress helper.
    """
    chnl = Channel()
    chnl.end()
    with channel.Finished.suppress():
        chnl.get()
    chnl = Channel()
    chnl.cancel()
    with channel.Cancelled.suppress():
        chnl.get()

def test_suppressed_decorator():
    """
    Channel exception suppression also works as a decorator.
    """
    @channel.Finished.suppress()
    def get():
        chnl.get()
    chnl = Channel()
    chnl.end()
    get()
    chnl = Channel()
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        get()
    channel.Cancelled.suppress()(get)()

def test_end_states():
    """
    Once cancelled, a Channel stays that way. If ended, though,
    it can be cancelled to prevent any remaining data being read.
    """
    chnl = Channel()
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        chnl.put(1)
    with pytest.raises(channel.Cancelled):
        chnl.get()
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        chnl.get()
    chnl.end()
    with pytest.raises(channel.Cancelled):
        chnl.get()

    chnl = Channel()
    chnl.end()
    with pytest.raises(channel.Finished):
        chnl.get()
    with pytest.raises(channel.Finished):
        chnl.put(1)
    chnl.end()
    with pytest.raises(channel.Finished):
        chnl.get()
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        chnl.get()

    chnl = Channel()
    chnl.put_many([1, 2, 3])
    chnl.end()
    assert chnl.get() == 1
    chnl.cancel()
    with pytest.raises(channel.Cancelled):
        chnl.get()

def test_wrapped_channel():
    """
    We can wrap a channel to modify what gets read or written
    """
    class TestChannel(channel.WrappedChannel):
        def put(self, value):
            if value % 2 == 1:
                super().put(value)
        def get(self):
            while True:
                raw = super().get()
                if raw < 5:
                    break
            return raw ** 2

    chnl = TestChannel(Channel())
    chnl.put_many([1, 2, 3, 4, 5]).end()
    assert chnl.get() == 1
    assert chnl.get() == 9
    with pytest.raises(channel.Finished):
        chnl.get()
    with pytest.raises(channel.Finished):
        chnl.check()

def test_wrapped_close():
    """
    WrappedChannel has a method you can overload to be notified when either
    end() or cancel() is called, instead of having to overload both.
    """
    class TestChannel(channel.WrappedChannel):
        def __init__(self, other):
            super().__init__(other)
            self.close_called = False
        def close(self):
            self.close_called = True

    chnl = TestChannel(Channel())
    chnl.put_many([1, 2, 3])
    assert not chnl.close_called
    chnl.end()
    assert chnl.close_called
    assert list(chnl) == [1, 2, 3]

    chnl = TestChannel(Channel())
    chnl.put_many([1, 2, 3])
    assert not chnl.close_called
    chnl.cancel()
    assert chnl.close_called
    assert list(chnl) == []

def test_thread_source():
    """
    thread_source starts a thread which writes items to a Channel.
    """
    # This is the basic behaviour.
    chnl = channel.thread_source(range(10))
    assert list(chnl) == list(range(10))
    # Now test that it really is running in another thread.
    def source():
        yield threading.get_ident()
    chnl = channel.thread_source(source())
    assert chnl.get() != threading.get_ident()
    # Test that cancelling the channel ends the thread.
    success = threading.Event()
    delta = 0.1
    def source():
        yield 1
        try:
            while True:
                time.sleep(delta)
                yield 2
        except GeneratorExit:
            # If we make it here, GeneratorExit was raised from yield,
            # meaning the iterator was garbage collected, meaning the thread
            # that was running it is finished.
            success.set()
    with channel.thread_source(source()).cancel_context() as chnl:
        assert len(threading.enumerate()) == 2
        assert chnl.get() == 1
    # wait() returns False if it times out
    assert success.wait(10 * delta)
    assert len(threading.enumerate()) == 1

def test_raising_iterable():
    """
    If the iterable for a thread_source raises, the Channel is cancelled.
    """
    # We could actually use a Channel for the notification, but let's keep
    # it clear what Channel we're testing by only having one.
    ready = threading.Event()
    def raiser():
        yield 1
        ready.wait()
        raise Exception()
    chnl = channel.thread_source(raiser())
    assert chnl.get() == 1
    ready.set()
    with pytest.raises(channel.Cancelled):
        chnl.get()

def test_thread_source_channel():
    """
    Everything thread_source does, it can do when the output channel is
    specified by the user insted of created by the call to thread_source()
    """
    my_chnl = Channel()
    my_chnl.put_many(range(5))
    # This is the basic behaviour.
    chnl = channel.thread_source(range(10), channel=my_chnl)
    assert chnl is my_chnl
    assert list(chnl) == list(range(5)) + list(range(10))
    # TODO: we could build this test out to do more of what test_thread_source
    # and test_raising_iterable do.

def run_daemon():  # pragma: no cover: runs in subprocess
    def forever():
        while True:
            time.sleep(10)
            yield 1
    channel.thread_source(forever(), daemon=True)
    raise SystemExit(23)

def test_thread_daemon():
    """
    Daemon thread does not prevent process exit.
    """
    proc = multiprocessing.Process(target=run_daemon, daemon=True)
    proc.start()
    proc.join(1)
    assert proc.exitcode == 23

@pytest.mark.parametrize('count', [1, 5, 100])
def test_thread_crew(count):
    """
    thread_crew() creates multiple worker threads, reading from one Channel
    and writing to another.
    """
    def worker(value, check, out_chnl):
        out_chnl.put((value, threading.get_ident()))
        if value == count - 1:
            out_chnl.end()
        # Now sleep until the work is finished, so that every value is run by a
        # different thread, and hence we can test the number of threads.
        while True:
            out_chnl.check()
            time.sleep(0.1)
    requests, responses = channel.thread_crew(count, worker, mode='1:m')
    requests.put_many(range(count)).end()
    outputs = tuple(responses)
    assert len(outputs) == count
    assert {output[0] for output in outputs} == set(range(count))
    assert len({output[1] for output in outputs}) == count

@pytest.mark.parametrize('count', [1, 5, 100])
def test_thread_crew_mode(count):
    """
    thread_crew_mode() can be any of '1:1', '1:m', 'm:m'. In 1:1 mode, the
    worker function is simply called with a value read off one Channel, and
    returns a value to be written to the other. In '1:m' mode, it is passed a
    value plus the output Channel, and writes as many values as it likes. In
    m:m mode it is passed both Channels, and has full control.
    """
    def worker_1_1(value, check_cancelled):
        return 2 * value
    def worker_1_m(value, check_cancelled, out_chnl):
        out_chnl.put(2 * value)
        out_chnl.put(2 * value + 1)
    def worker_m_m(in_chnl, out_chnl):
        while True:
            value = in_chnl.get()
            out_chnl.put_many(range(3 * value, 3 * value + 3))
    # The default is 1:1
    requests, responses = channel.thread_crew(count, worker_1_1)
    requests.put_many(range(1000)).end()
    assert set(responses) == set(range(0, 2000, 2))
    requests, responses = channel.thread_crew(count, worker_1_1, mode='1:1')
    requests.put_many(range(1000)).end()
    assert set(responses) == set(range(0, 2000, 2))
    # Other workers produce different results
    requests, responses = channel.thread_crew(count, worker_1_m, mode='1:m')
    requests.put_many(range(1000)).end()
    assert set(responses) == set(range(2000))
    requests, responses = channel.thread_crew(count, worker_m_m, mode='m:m')
    requests.put_many(range(1000)).end()
    assert set(responses) == set(range(3000))
    with pytest.raises(ValueError):
        channel.thread_crew(count, worker_1_1, mode='bad_value')

@pytest.mark.parametrize('cancel_requests', [True, False])
def test_thread_crew_cancel(cancel_requests):
    """
    Cancelling either channel cancels the Crew.
    """
    def worker_1_1(value, check_cancelled):
        while True:
            check_cancelled()
            time.sleep(0.1)
    requests, responses = channel.thread_crew(100, worker_1_1)
    requests.put_many(range(100))
    def do_cancel():
        if cancel_requests:
            requests.cancel()
        else:
            responses.cancel()
    def check_is_cancelled():
        with pytest.raises(channel.Cancelled):
            if cancel_requests:
                # Blocks until the channel is cancelled by the crew.
                responses.get()
            else:
                # 10 seconds should be plenty of time for the crew to respond.
                for _ in range(100):  # pragma: no branch
                    time.sleep(0.1)
                    requests.put(None)
    do_cancel()
    check_is_cancelled()
    # The same test again in 1:m mode. This time we can write the output
    # before going to sleep, which means we can test everything gets done.
    def worker_1_m(value, check_cancelled, responses):
        responses.put(value)
        worker_1_1(value, check_cancelled)
    requests, responses = channel.thread_crew(100, worker_1_m, mode='1:m')
    requests.put_many(range(100))
    assert set(itertools.islice(responses, 100)) == set(range(100))
    do_cancel()
    check_is_cancelled()

@pytest.mark.parametrize('count', [1, 5, 100])
def test_thread_crew_exception(count):
    """
    As with thread_source(), if the thread raises then everything is cancelled.
    """
    def worker_1_1(value, check_cancelled):
        if value == 99:
            raise ValueError(value)
    requests, responses = channel.thread_crew(count, worker_1_1)
    requests.put_many(range(100))
    with pytest.raises(channel.Cancelled):
        # We could get up to 99 values out, but by then it must be cancelled.
        for _ in range(100):  # pragma: no branch
            responses.get()
    with pytest.raises(channel.Cancelled):
        # 10 seconds should be plenty of time for the crew to respond.
        for _ in range(100):  # pragma: no branch
            time.sleep(0.1)
            requests.put(None)

@pytest.mark.parametrize('count', [1, 5, 100])
def test_thread_crew_channel(count):
    """
    Everything thread_crew does, it can do when the output channel and/or input
    channel is specified by the user insted of created by the call to
    thread_crew()
    """
    # TODO as with thread_source, we just test the basic behaviour, but it
    # might be worth extending this to cover more of the thread_crew tests.
    def worker(value, check, out_chnl):
        out_chnl.put((value, threading.get_ident()))
        if value == count - 1:
            out_chnl.end()
        # Now sleep until the work is finished, so that every value is run by a
        # different thread, and hence we can test the number of threads.
        while True:
            out_chnl.check()
            time.sleep(0.1)
    my_requests = Channel()
    my_responses = Channel()
    requests, responses = channel.thread_crew(count, worker, mode='1:m', requests=my_requests, responses=my_responses)
    assert requests is my_requests
    assert responses is my_responses
    requests.put_many(range(count)).end()
    outputs = tuple(responses)
    assert len(outputs) == count
    assert {output[0] for output in outputs} == set(range(count))
    assert len({output[1] for output in outputs}) == count
