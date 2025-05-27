words = ["hi", "hello", "cat", "a", "sun"]
grouped = {}
for word in words:
    l = len(word)
    grouped.setdefault(l, []).append(word)
print(grouped)
