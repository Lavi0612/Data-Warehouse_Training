class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity > self.in_stock:
            raise ValueError("Not enough stock!")
        self.in_stock -= quantity

try:
    title = input("Book title: ")
    author = input("Author: ")
    price = float(input("Price: "))
    stock = int(input("Stock quantity: "))
    book = Book(title, author, price, stock)
    qty = int(input("How many to sell? "))
    book.sell(qty)
    print(f"Sold {qty} book(s). Remaining stock: {book.in_stock}")
except ValueError as e:
    print("Error:", e)
print()