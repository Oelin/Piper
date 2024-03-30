from typing import Any, Iterable

import mpipe


def Pipeline(
    workers: Iterable[mpipe.OrderedWorker],
    sizes: Iterable[int],
) -> mpipe.Pipeline:
    """Pipeline.

    Parameters
    ----------
    workers : Iterable[mpipe.OrderedWorker]
        The workers.
    sizes : int
        The sizes for each stage.

    Returns
    -------
    pipeline : mpipe.Pipeline
        The pipeline.
    """

    stages = [mpipe.Stage(worker, size) for worker, size in zip(workers, sizes)]

    for stage, next in zip(stages[:-1], stages[1:]):
        stage.link(next)

    pipeline = mpipe.Pipeline(stages[0])

    return pipeline


def apply(results: Iterable[Any], pipeline: mpipe.Pipeline) -> Iterable[Any]:
    """Apply.

    Parameters
    ----------
    results : Iterable[Any]
        The input results.
    pipeline : mpipe.Pipeline
        The pipeline.

    Returns
    -------
    results : Iterable[Any]
        The output results.
    """

    for result in results:
        pipeline.put(result)

    pipeline.put(None)

    yield from pipeline.results()
