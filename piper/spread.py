from typing import Any, Callable, Iterable

import mpipe


def Spread(
    function: Callable[[Any, ...], Iterable[Any]],
) -> mpipe.OrderedWorker:
    """Spread.

    Parameters
    ----------
    function : Callable[[Any, ...], Iterable[Any]]
        The function.

    Returns
    -------
    worker : mpipe.OrderedWorker
        The worker.
    """

    class Worker(mpipe.OrderedWorker):
        def doTask(self, result: Iterable[Any]) -> None:

            for element in function(result):
                self.putResult(element)

    return Worker()
