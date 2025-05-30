# q14_load_csvs.py
import pandas as pd

emp = pd.read_csv("employees.csv")
proj = pd.read_csv("projects.csv")
print(emp.head())
print(proj.head())
