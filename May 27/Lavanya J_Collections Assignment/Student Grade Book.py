grades = {}
for _ in range(3):
    name = input("Enter name: ")
    marks = float(input("Enter marks: "))
    if marks >= 90:
        grade = "A"
    elif marks >= 75:
        grade = "B"
    else:
        grade = "C"
    grades[name] = grade

search = input("Enter student name to check grade: ")
print(f"{search}'s grade: {grades.get(search, 'Not found')}")
