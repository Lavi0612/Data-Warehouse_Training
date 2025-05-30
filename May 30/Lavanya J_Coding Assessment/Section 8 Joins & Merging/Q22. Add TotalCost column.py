# q22_add_total_cost.py
import pandas as pd

emp = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
proj = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/projects.csv")

merged = pd.merge(emp, proj, on="EmployeeID")
merged['TotalCost'] = merged['HoursAllocated'] * (merged['Salary'] / 160)
print(merged[['Name', 'ProjectName', 'TotalCost']])
