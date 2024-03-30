from typing import Any, Callable, Iterable

import mpipe


def Reduce(
    function: Callable[[Any, ...], Any],
    result: Any,
) -> mpipe.OrderedWorker:
    """Reduce.

    Parameters
    ----------
    function : Callable[[Any, ...], Any]
        The function.
    result : Any
        The initial result.

    Returns
    -------
    worker : mpipe.OrderedWorker
        The worker.
    """

    class Worker(mpipe.OrderedWorker):
        def doTask(self, result_prime: Any) -> None:
            nonlocal result

            result = function(result_prime, result)
            self.putResult(result)

    return Worker()
