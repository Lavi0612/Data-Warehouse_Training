# q15_explore_employees.py
import pandas as pd

df = pd.read_csv("employees.csv")
print(df.head(2))
print("Departments:", df['Department'].unique())
print(df.groupby('Department')['Salary'].mean())
