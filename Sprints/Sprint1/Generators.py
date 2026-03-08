# fill in this function
def fib():
    a, b = 0, 1
    while 1:
        yield a
        a, b = b, a + b

# testing code
import types
if isinstance(fib(), types.GeneratorType):
    print("Good, The fib function is a generator.")

    counter = 0
    for n in fib():
        print(n)
        counter += 1
        if counter == 10:
            break

#this is fibonacci generator and the fact here works that each time we call next() on the generator it resumes from where it last yielded a value.
#Yield basically allows the function to produce a series of values over time, pausing after each yield and resuming from that point on the next call.
#Thus it is helpful for saving computation and memory when dealing with large sequences.