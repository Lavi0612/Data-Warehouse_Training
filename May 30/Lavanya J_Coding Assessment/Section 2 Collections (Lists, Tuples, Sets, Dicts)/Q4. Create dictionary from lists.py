# q4_dict_from_lists.py
keys = input("Enter comma-separated keys: ").split(',')
values = list(map(int, input("Enter comma-separated values: ").split(',')))
my_dict = dict(zip(keys, values))
print("Dictionary:", my_dict)