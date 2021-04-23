import time
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
