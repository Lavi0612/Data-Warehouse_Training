# q16_add_tenure.py
import pandas as pd
from datetime import datetime

df = pd.read_csv("employees.csv")
df['TenureInYears'] = datetime.now().year - pd.to_datetime(df['JoiningDate']).dt.year
print(df[['Name', 'TenureInYears']])
