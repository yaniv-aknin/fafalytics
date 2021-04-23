import time
import contextlib
from typing import Callable, Iterable

def wait(iterations: int, interval: float, error: Exception=TimeoutError(), predicate: Callable[[], bool]=lambda: False) -> Iterable[int]:
    for iteration in range(iterations):
        time.sleep(interval)
        if predicate():
            break
        yield
    else:
        raise error

def block_wait(iterations: int, interval: float, error: Exception=TimeoutError(), predicate: Callable[[], bool]=lambda: False) -> None:
    for x in wait(iterations, interval, error, predicate):
        pass

def negate(f: Callable[[], bool]) -> bool:
    return lambda: not(f())

def first(iterable):
    return next(iter(iterable))

class Timer:
    def __init__(self):
        self.start = None
        self.end = None
    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *exc):
        self.end = time.time()
    @property
    def running(self):
        return self.start and not self.end
    @property
    def elapsed(self):
        if not self.start:
            raise ValueError('not started')
        end = self.end or time.time()
        return end-self.start
