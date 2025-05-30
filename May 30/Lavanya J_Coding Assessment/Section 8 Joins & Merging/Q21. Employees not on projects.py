# q21_find_unassigned.py
import pandas as pd

emp = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
proj = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/projects.csv")
merged = pd.merge(emp, proj, on="EmployeeID", how="left")
not_assigned = merged[merged['ProjectID'].isna()]
print(not_assigned[['EmployeeID', 'Name']])
