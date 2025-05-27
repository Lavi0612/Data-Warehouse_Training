nums = [10, 20, 4, 45, 99]
first = second = float('-inf')
for n in nums:
    if n > first:
        second = first
        first = n
    elif first > n > second:
        second = n
print("Second largest:", second)
