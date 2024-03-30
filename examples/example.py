from piper import *

pipeline = Pipeline(
    workers=[
	Map(lambda x: x + 1),
	Map(lambda x: x * 2),
	Reduce(lambda a, b: a + b, 0),
	Spread(lambda a: [a, a]),
	Unspread(4),
    ],
    sizes=(1,1, 1, 1, 1, 1),
)

print(list(apply([1,2,3, 4, 5, 6, 7, 8], pipeline)))
