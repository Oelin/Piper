from typing import Any, Callable

import mpipe


def Filter(predictate: Callable[[Any, ...], bool]) -> mpipe.OrderedWorker:
    """Filter.

    Parameters
    ----------
    predicate : Callable[[Any, ...], bool]
        The predicate.

    Returns
    -------
    worker : mpipe.OrderedWorker
        The worker.
    """

    class Worker(mpipe.OrderedWorker):
        def doTask(self, result: Any) -> None:

            if predicate(result):
                self.putResult(result)

    return Worker()
