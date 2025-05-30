# q19_sort_by_salary.py
import pandas as pd

df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
sorted_df = df.sort_values(by='Salary', ascending=False)
print(sorted_df)
