from typing import Any, Callable

import mpipe


def Map(function: Callable[[Any, ...], Any]) -> mpipe.OrderedWorker:
    """Map.

    Parameters
    ----------
    function : Callable[[Any, ...], Any]
        The function.

    Returns
    -------
    worker : mpipe.OrderedWorker
        The worker.
    """

    class Worker(mpipe.OrderedWorker):
        def doTask(self, result: Any) -> None:

            self.putResult(function(result))

    return Worker()
