class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        avg = self.average()
        if avg >= 90:
            return 'A'
        elif avg >= 75:
            return 'B'
        elif avg >= 50:
            return 'C'
        else:
            return 'F'

name = input("Student name: ")
marks = list(map(float, input("Enter marks separated by space: ").split()))
student = Student(name, marks)
print(f"Average: {student.average():.2f}, Grade: {student.grade()}")
print()