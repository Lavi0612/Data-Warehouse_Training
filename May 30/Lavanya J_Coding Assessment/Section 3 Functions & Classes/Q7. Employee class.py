# q7_employee_class.py
import pandas as pd

df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")

class Employee:
    def __init__(self, emp_id, name, salary):
        self.emp_id = emp_id
        self.name = name
        self.salary = salary

    def display(self):
        print(f"\nEmployee ID: {self.emp_id}")
        print(f"Name: {self.name}")
        print(f"Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary > 60000

print("Available Employee IDs:")
print(df[['EmployeeID', 'Name']])

emp_id = int(input("\nEnter EmployeeID to view details: "))

if emp_id in df['EmployeeID'].values:
    row = df[df['EmployeeID'] == emp_id].iloc[0]
    emp = Employee(row['EmployeeID'], row['Name'], row['Salary'])
    emp.display()
    print("High Earner?", "Yes" if emp.is_high_earner() else "No")
else:
    print("Employee ID not found.")
