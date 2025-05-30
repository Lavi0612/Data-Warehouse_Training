# q13_divide_with_error.py
def divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Can't divide by zero!"

a = float(input("Enter numerator: "))
b = float(input("Enter denominator: "))
print("Result:", divide(a, b))

