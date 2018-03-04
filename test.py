from cachetools import cached

@cached(cache={})
def fib(n):
    return n if n < 2 else fib(n - 1) + fib(n - 2)

for i in range(100):
    print('fib(%d) = %d' % (i, fib(i)))