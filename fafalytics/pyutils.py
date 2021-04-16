import time
from typing import Callable, Iterable

def wait(iterations: int, interval: float, message: str='', predicate: Callable[[], bool]=lambda: False) -> Iterable[int]:
    for iteration in range(iterations):
        time.sleep(interval)
        if predicate():
            break
        yield
    else:
        raise TimeoutError(message)

def block_wait(iterations: int, interval: float, message: str='', predicate: Callable[[], bool]=lambda: False) -> None:
    for x in wait(iterations, interval, message, predicate):
        pass

def negate(f: Callable[[], bool]) -> bool:
    return lambda: not(f())

def query_dict(d, query):
    if isinstance(query, tuple):
        query, postprocess = query
    else:
        postprocess = lambda x: x
    components = query.split('/')
    for component in components:
        if component.isdigit():
            component = int(component)
        d = d[component]
    return postprocess(d)
