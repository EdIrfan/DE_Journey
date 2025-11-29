my_strings = ['a', 'b', 'c', 'd', 'e']
my_numbers = [1, 2, 3, 4, 5]

results = [(x, y) for x, y in zip(my_strings, my_numbers)]
#the result is equivalent to ----
#results = list(map(lambda x, y: (x, y), my_strings, my_numbers))
print(results)

results = list(zip(my_strings, my_numbers))

print(results)