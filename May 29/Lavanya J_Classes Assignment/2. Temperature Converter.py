def convert_temp(value, unit):
    if unit.upper() == 'C':
        return (value * 9/5) + 32
    elif unit.upper() == 'F':
        return (value - 32) * 5/9
    else:
        return "Invalid unit"

try:
    value = float(input("Enter temperature value: "))
    unit = input("Enter unit (C/F): ")
    result = convert_temp(value, unit)
    print("Converted temperature:", result)
except ValueError:
    print("Invalid temperature input.")
print()