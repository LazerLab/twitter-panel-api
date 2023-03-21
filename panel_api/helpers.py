from typing import Optional, TypeVar, Callable

T = TypeVar("T")
U = TypeVar("U")


def if_present(callable: Callable[[T], U], optional: Optional[T]) -> Optional[U]:
    """
    Call a function on a value if it is not None. Otherwise, just return None.
    """
    if optional is None:
        return None
    return callable(optional)
