class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

class Employee(Person):
    def __init__(self, name, age, emp_id, salary):
        super().__init__(name, age)
        self.emp_id = emp_id
        self.salary = salary

    def display_info(self):
        print(f"Name: {self.name}, Age: {self.age}, ID: {self.emp_id}, Salary: {self.salary}")

emp_name = input("Employee name: ")
emp_age = int(input("Age: "))
emp_id = input("Employee ID: ")
emp_salary = float(input("Salary: "))
emp = Employee(emp_name, emp_age, emp_id, emp_salary)
emp.display_info()
print()