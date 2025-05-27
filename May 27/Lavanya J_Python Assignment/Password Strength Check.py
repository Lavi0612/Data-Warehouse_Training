import re
password = input("Enter a password: ")

length = len(password) >= 8
digit = re.search(r"\d", password)
upper = re.search(r"[A-Z]", password)
symbol = re.search(r"[!@#$%^&*(),.?\":{}|<>]", password)

if length and digit and upper and symbol:
    print("Strong password")
else:
    print("Weak password")
