import pandas as pd
import numpy as np

df = pd.read_csv("supply_chain_data.csv")

df_cleaned = df.dropna(subset=['product_name', 'supplier_name', 'order_date', 'delivery_date']).copy()

df_cleaned['order_date'] = pd.to_datetime(df_cleaned['order_date'], errors='coerce')
df_cleaned['delivery_date'] = pd.to_datetime(df_cleaned['delivery_date'], errors='coerce')

df_cleaned = df_cleaned.dropna(subset=['order_date', 'delivery_date'])

today = pd.Timestamp.today()
df_cleaned['delay_days'] = (today - df_cleaned['delivery_date']).dt.days
df_cleaned['is_delayed'] = np.where(df_cleaned['delay_days'] > 0, 1, 0)

df_cleaned['needs_reorder'] = np.where(df_cleaned['quantity'] < df_cleaned['reorder_level'], 1, 0)

total_quantity = np.sum(df_cleaned['quantity'])
avg_quantity = np.mean(df_cleaned['quantity'])
std_quantity = np.std(df_cleaned['quantity'])
max_quantity = np.max(df_cleaned['quantity'])
min_quantity = np.min(df_cleaned['quantity'])
percent_delayed = np.mean(df_cleaned['is_delayed']) * 100

print("Supply Chain Quantity Statistics:")
print(f"Total Quantity: {total_quantity}")
print(f"Average Quantity: {avg_quantity:.2f}")
print(f"Standard Deviation: {std_quantity:.2f}")
print(f"Max Quantity: {max_quantity}")
print(f"Min Quantity: {min_quantity}")
print(f"Delayed Orders: {percent_delayed:.2f}%")

# Cumulative quantity ordered over time
df_cleaned = df_cleaned.sort_values(by='order_date')
df_cleaned['cumulative_quantity'] = np.cumsum(df_cleaned['quantity'])

# Select final processed columns
df_processed = df_cleaned[[
    'order_id', 'product_name', 'supplier_name', 'order_date', 'delivery_date',
    'quantity', 'reorder_level', 'status',
    'delay_days', 'is_delayed', 'needs_reorder', 'cumulative_quantity'
]]

# Save cleaned data to CSV
df_processed.to_csv("cleaned_supply_chain_data.csv", index=False)
print(" Cleaned and processed data saved to 'cleaned_supply_chain_data.csv'")
