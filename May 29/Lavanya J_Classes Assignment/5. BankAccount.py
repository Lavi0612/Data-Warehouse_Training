class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount

    def withdraw(self, amount):
        if amount > self.balance:
            print("Insufficient funds!")
        else:
            self.balance -= amount

    def get_balance(self):
        return self.balance

name = input("Enter account holder name: ")
acc = BankAccount(name)
acc.deposit(float(input("Deposit amount: ")))
acc.withdraw(float(input("Withdraw amount: ")))
print("Current balance:", acc.get_balance())
print()
