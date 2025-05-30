# q11_word_count.py
file = input("Enter filename: ")
with open(file, "r") as f:
    words = f.read().split()
    print("Word count:", len(words))
