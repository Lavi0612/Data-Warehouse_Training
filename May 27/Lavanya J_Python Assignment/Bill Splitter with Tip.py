bill = float(input("Enter total bill amount: "))
people = int(input("Enter number of people: "))
tip_percent = float(input("Enter tip percentage: "))

total_with_tip = bill + (bill * tip_percent / 100)
per_person = total_with_tip / people

print(f"Each person should pay: {per_person:.2f}")
