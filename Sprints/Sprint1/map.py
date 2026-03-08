circle_areas = [3.56773, 5.57668, 4.00914, 56.24241, 9.01344, 32.00013]

result = list(map(round, circle_areas, range(1, 7)))

print(result)

result2 = list(map(round, circle_areas, range(1, 4)))

print(result2)

#The map function makes --- function x iterable.
#if no iterable it will simply not print and no error is thrown.