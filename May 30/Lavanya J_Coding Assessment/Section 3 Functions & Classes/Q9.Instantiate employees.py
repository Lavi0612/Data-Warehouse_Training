# q9_test_employees.py
import pandas as pd

employees_df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")

class Employee:
    def __init__(self, emp_id, name, salary):
        self.emp_id = emp_id
        self.name = name
        self.salary = salary

    def is_high_earner(self):
        return self.salary > 60000

employees = []
print("Available Employee IDs:", list(employees_df['EmployeeID']))
for i in range(3):
    emp_id = int(input(f"\nEnter EmployeeID {i+1}: "))
    if emp_id in employees_df['EmployeeID'].values:
        row = employees_df[employees_df['EmployeeID'] == emp_id].iloc[0]
        emp = Employee(emp_id, row['Name'], row['Salary'])
        employees.append(emp)
    else:
        print("EmployeeID not found.")

print("\nHigh Earner Status:")
for emp in employees:
    status = "Yes" if emp.is_high_earner() else "No"
    print(f"{emp.name} (ID {emp.emp_id}) → High Earner? {status}")
