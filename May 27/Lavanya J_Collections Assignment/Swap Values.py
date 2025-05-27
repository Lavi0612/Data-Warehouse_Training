def swap(t1, t2):
    return t2, t1

a = (1, 2)
b = (3, 4)
a, b = swap(a, b)
print(a, b)
