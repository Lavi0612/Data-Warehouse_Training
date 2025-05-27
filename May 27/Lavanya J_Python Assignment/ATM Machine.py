balance = 10000

while True:
    print("\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Choose an option: ")

    if choice == "1":
        amount = float(input("Enter amount to deposit: "))
        balance += amount
    elif choice == "2":
        amount = float(input("Enter amount to withdraw: "))
        if amount <= balance:
            balance -= amount
        else:
            print("Insufficient balance")
    elif choice == "3":
        print("Balance:", balance)
    elif choice == "4":
        break
    else:
        print("Invalid option")
