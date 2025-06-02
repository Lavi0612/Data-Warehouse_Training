import pandas as pd
import numpy as np

# 1. Load CSV files
orders_df = pd.read_csv('orders.csv')
status_df = pd.read_csv('delivery_status.csv')
customers_df = pd.read_csv('customers.csv')

# 2. Clean data: remove rows missing critical values
orders_df.dropna(subset=['customer_id', 'order_date', 'delivery_date'], inplace=True)

# Type conversions
orders_df['customer_id'] = orders_df['customer_id'].astype(int)
orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
orders_df['delivery_date'] = pd.to_datetime(orders_df['delivery_date'], errors='coerce')

# 3. Calculate delivery delay (in days) and delay flag
orders_df['delay_days'] = (pd.Timestamp.today().normalize() - orders_df['delivery_date']).dt.days
orders_df['delayed'] = np.where(orders_df['delay_days'] > 0, 1, 0)

# 4. Merge with delivery issues
merged_df = pd.merge(orders_df, status_df, on='order_id', how='left')

# 5. Merge with customer names (optional for reporting)
merged_df = pd.merge(merged_df, customers_df[['customer_id', 'name']], on='customer_id', how='left')

# 6. Summary: top delayed customers
delay_summary = merged_df.groupby(['customer_id', 'name'])['delayed'].sum().reset_index()
delay_summary = delay_summary.sort_values(by='delayed', ascending=False)

# 7. Common delivery issues
issue_counts = merged_df['issue'].dropna().str.lower().value_counts().reset_index()
issue_counts.columns = ['issue', 'count']

# 8. Output
print("🚨 Top Delayed Customers")
print(delay_summary.head(10))

print("\n⚠️ Most Common Delivery Issues")
print(issue_counts.head(10))

# 9. Save cleaned and summary data
merged_df.to_csv('cleaned_orders_data.csv', index=False)
delay_summary.to_csv('delay_summary_by_customer.csv', index=False)
issue_counts.to_csv('delivery_issues_report.csv', index=False)
