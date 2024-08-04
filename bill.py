import dask.dataframe as dd
import json
import time

start_time = time.time()
df = dd.read_parquet('CUR10MB.parquet')

total_cost = df["line_item_blended_cost"].sum().compute()
tax_cost = df[df['line_item_line_item_description'].str.contains('Tax', case=False, na=False)]['line_item_blended_cost'].sum().compute()
usage_cost = total_cost-tax_cost

end_time = time.time()

cost_details = {
    "total_cost": f"{total_cost:.2f}",
    "tax_cost": f"{tax_cost:.2f}",
    "usage_cost": f"{usage_cost:.2f}"

}
data = {
    "cost_details": cost_details
}

with open('OUTPUT.JSON', 'w') as json_file:
    json.dump(data, json_file, indent=2)
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time} seconds")
   