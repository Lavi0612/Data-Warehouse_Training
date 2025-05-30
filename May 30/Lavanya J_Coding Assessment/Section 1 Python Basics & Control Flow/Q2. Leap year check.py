# q2_leap_year.py
year = int(input("Enter a year: "))
def is_leap(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

print("Leap year?" , is_leap(year))
