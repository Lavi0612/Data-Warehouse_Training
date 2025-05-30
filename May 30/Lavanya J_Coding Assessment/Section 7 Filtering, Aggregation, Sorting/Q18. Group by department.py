# q18_group_by_department.py
import pandas as pd

df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
grouped = df.groupby('Department').agg(
    Count=('EmployeeID', 'count'),
    TotalSalary=('Salary', 'sum'),
    AvgSalary=('Salary', 'mean')
)
print(grouped)
