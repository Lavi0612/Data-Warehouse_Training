# q20_merge_projects.py
import pandas as pd

emp = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
proj = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/projects.csv")
merged = pd.merge(emp, proj, on="EmployeeID")
print(merged)
