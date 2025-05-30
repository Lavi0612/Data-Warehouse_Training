# q10_write_it_names.py
import pandas as pd

df = pd.read_csv("../Section 6 Pandas – Reading & Exploring CSVs/employees.csv")
it_emps = df[df['Department'] == 'IT']

with open("it_employees.txt", "w") as f:
    for name in it_emps['Name']:
        f.write(name + "\n")

print("IT employee names written to it_employees.txt")