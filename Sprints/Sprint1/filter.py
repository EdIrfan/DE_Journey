scores = [66, 90, 68, 59, 76, 60, 88, 74, 81, 65]

def is_A_student(score):
    return score > 75

over_75 = list(filter(is_A_student, scores))

print(over_75)


#filter function filters an iterable (like a list) by applying a function that 
#returns either True or False to each element and only returns those elements for which the function returns True.