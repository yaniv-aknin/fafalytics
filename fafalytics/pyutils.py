import time
import click
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

class EchoTimer(Timer):
    def __init__(self, message):
        super().__init__()
        self.message = message
    def __enter__(self):
        super().__enter__()
        click.echo(self.message+' ', nl=False)
    def __exit__(self, *exc):
        super().__exit__(*exc)
        if not any(exc):
            click.echo('(%.2fs)' % self.elapsed)

class Literal:
    def __init__(self, value):
        self.value = value
class Query:
    def __init__(self, path, missing=None, cast=None, reraise=KeyError):
        self.path = path
        self.missing = missing
        self.cast = cast
        self.reraise = reraise
    def __call__(self, obj):
        if isinstance(self.path, Literal):
            return self.path.value
        for component in self.path.split('/'):
            try:
                obj = obj[component]
            except KeyError as error:
                if self.missing:
                    return self.missing(obj, component)
                if self.reraise is KeyError:
                    raise
                else:
                    raise self.reraise from error
                raise
        return obj if self.cast is None else self.cast(obj)

def restructure_dict(src, queries):
    dst = {}
    for dst_path, query in queries.items():
        dst_obj = dst
        components = dst_path.split('/')
        prefix, key = components[:-1], components[-1]
        for component in prefix:
            dst_obj = dst_obj.setdefault(component, {})
        dst_obj[key] = query(src)
    return dst
