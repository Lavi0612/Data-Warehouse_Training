# q8_project_subclass.py
import pandas as pd

employees_df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
projects_df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/projects.csv")

class Employee:
    def __init__(self, emp_id, name, salary):
        self.emp_id = emp_id
        self.name = name
        self.salary = salary

class Project(Employee):
    def __init__(self, emp_id, name, salary, project_name, hours_allocated):
        super().__init__(emp_id, name, salary)
        self.project_name = project_name
        self.hours_allocated = hours_allocated

    def show_details(self):
        print(f"\nEmployee ID: {self.emp_id}")
        print(f"Name: {self.name}")
        print(f"Salary: {self.salary}")
        print(f"Project: {self.project_name}")
        print(f"Hours Allocated: {self.hours_allocated}")

emp_id = int(input("Enter EmployeeID to create Project object: "))

if emp_id in employees_df['EmployeeID'].values:
    emp_row = employees_df[employees_df['EmployeeID'] == emp_id].iloc[0]
    name = emp_row['Name']
    salary = emp_row['Salary']
else:
    print("EmployeeID not found in employees.csv.")
    exit()

project_name = input("Enter project name: ")
hours = int(input("Enter hours allocated: "))

proj = Project(emp_id, name, salary, project_name, hours)
proj.show_details()
