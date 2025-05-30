# q17_filter_it_emps.py
import pandas as pd

df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
filtered = df[(df['Department'] == 'IT') & (df['Salary'] > 60000)]
print(filtered)
