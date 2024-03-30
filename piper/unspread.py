from typing import Any, Iterable

import mpipe


def Unspread(size: int) -> mpipe.OrderedWorker:
    """Unspread.

    Returns
    -------
    worker : mpipe.OrderedWorker
        The worker.
    """

    chunk = []

    class Worker(mpipe.OrderedWorker):
        def doTask(self, result: Any) -> None:

            if len(chunk) and not (len(chunk) % size):
                self.putResult(chunk.copy())
                chunk.clear()
            else:
                chunk.append(result)

    return Worker()
