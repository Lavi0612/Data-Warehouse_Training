a = {"x": 1, "y": 2}
b = {"y": 3, "z": 4}
merged = a.copy()
for k, v in b.items():
    merged[k] = merged.get(k, 0) + v
print(merged)
