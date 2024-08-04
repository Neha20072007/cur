import dask.dataframe as dd
import psutil
import time
import json

def get_cpu_usage():
    return psutil.cpu_percent(interval=1)

start_time = time.time()
df = dd.read_parquet('CUR10MB.parquet')

cpu_usage_before = get_cpu_usage()

total_cost = df['line_item_blended_cost'].sum().compute()
tax_cost = df[df['line_item_line_item_description'].str.contains('Tax', case=False, na=False)]['line_item_blended_cost'].sum().compute()
usage_cost = total_cost - tax_cost

cpu_usage_after = get_cpu_usage()
end_time = time.time()
execution_time = end_time - start_time

cost_details = {
    "total_cost": str(total_cost),
    "tax_cost": str(tax_cost),
    "usage_cost": str(usage_cost),
    "cpu_usage_before (%)": cpu_usage_before,
    "cpu_usage_after (%)": cpu_usage_after,
    "execution_time": execution_time
}

print(json.dumps(cost_details, indent=4))
