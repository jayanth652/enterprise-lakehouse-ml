import pandas as pd, numpy as np
from datetime import timedelta
from pathlib import Path
import random

out = Path("/data/raw_local")
out.mkdir(parents=True, exist_ok=True)

np.random.seed(7)
random.seed(7)

n_customers = 1000
customers = pd.DataFrame({
    "customer_id": np.arange(1, n_customers+1),
    "signup_date": pd.to_datetime("2023-01-01") + pd.to_timedelta(np.random.randint(0, 600, n_customers), unit="D"),
    "country": np.random.choice(["US","CA","UK","DE","IN","AU"], size=n_customers, p=[.45,.1,.15,.1,.15,.05]),
    "segment": np.random.choice(["A","B","C"], size=n_customers, p=[.4,.4,.2])
})
customers.to_csv(out / "customers.csv", index=False)

n_products = 200
products = pd.DataFrame({
    "product_id": np.arange(1, n_products+1),
    "category": np.random.choice(["Electronics","Books","Clothing","Home","Beauty"], size=n_products),
    "price": np.round(np.random.gamma(3, 20, size=n_products) + 5, 2)
})
products.to_csv(out / "products.csv", index=False)

n_orders = 15000
dates = [pd.to_datetime("2023-06-01") + timedelta(days=int(x)) for x in np.random.randint(0, 420, n_orders)]
orders = pd.DataFrame({
    "order_id": np.arange(1, n_orders+1),
    "customer_id": np.random.randint(1, n_customers+1, n_orders),
    "product_id": np.random.randint(1, n_products+1, n_orders),
    "order_date": dates,
    "quantity": np.random.choice([1,2,3], size=n_orders, p=[.7,.25,.05])
})
orders = orders.merge(products[["product_id","price"]], on="product_id", how="left")
orders["amount"] = orders["price"] * orders["quantity"]
orders.to_csv(out / "orders.csv", index=False)

n_events = 50000
event_ts = [pd.to_datetime("2023-06-01") + timedelta(minutes=int(x)) for x in np.random.randint(0, 420*24*60, n_events)]
clicks = pd.DataFrame({
    "event_id": np.arange(1, n_events+1),
    "customer_id": np.random.randint(1, n_customers+1, n_events),
    "event_time": event_ts,
    "event_type": np.random.choice(["view","add_to_cart","purchase"], size=n_events, p=[.75,.2,.05]),
    "session_id": np.random.randint(100000, 999999, n_events)
})
clicks.to_csv(out / "clickstream.csv", index=False)

print("Synthetic CSVs written to", out)