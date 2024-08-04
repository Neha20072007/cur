import dask.dataframe as dd
import time
import json

start_time = time.time()

df = dd.read_parquet('CUR10MB.parquet', columns=['line_item_blended_cost', 'line_item_line_item_description'])

total_cost = df['line_item_blended_cost'].sum().compute()
tax_cost = df[df['line_item_line_item_description'].str.contains('Tax', case=False, na=False)]['line_item_blended_cost'].sum().compute()
usage_cost = total_cost - tax_cost

end_time = time.time()
execution_time = end_time - start_time

res = {
    "cost_details": {
        "total_cost": f"{total_cost:.2f}",
        "tax_cost": f"{tax_cost:.2f}",
        "usage_cost": f"{usage_cost:.2f}"
    }
}

print(json.dumps(res, indent=4))

print(f"Execution Time: {execution_time:.2f} seconds")
