# q6_sets_difference.py
a = list(map(int, input("Enter list A: ").split(',')))
b = list(map(int, input("Enter list B: ").split(',')))
set_a = set(a)
set_b = set(b)
print("A - B:", set_a - set_b)
print("B - A:", set_b - set_a)
