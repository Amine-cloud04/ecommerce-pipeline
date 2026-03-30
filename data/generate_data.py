import csv, random, os
from datetime import datetime, timedelta

random.seed(42)

# --- Customers ---
with open("data/customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "name", "email", "city", "country"])
    cities = [("Casablanca","Morocco"),("Paris","France"),("Lyon","France"),
              ("Rabat","Morocco"),("Dubai","UAE"),("London","UK"),("Madrid","Spain")]
    for i in range(1, 201):
        city, country = random.choice(cities)
        w.writerow([i, f"Customer_{i}", f"user{i}@email.com", city, country])

# --- Products ---
with open("data/products.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["product_id", "name", "category", "price"])
    products = [
        ("Laptop","Electronics",999.99), ("Phone","Electronics",499.99),
        ("Headphones","Electronics",79.99), ("T-Shirt","Clothing",19.99),
        ("Jeans","Clothing",49.99), ("Sneakers","Clothing",89.99),
        ("Desk","Furniture",199.99), ("Chair","Furniture",149.99),
        ("Notebook","Stationery",4.99), ("Pen Set","Stationery",9.99),
        ("Backpack","Accessories",59.99), ("Watch","Accessories",199.99)
    ]
    for i, (name, cat, price) in enumerate(products, 1):
        w.writerow([i, name, cat, price])

# --- Orders (with some intentional nulls and duplicates for cleaning) ---
with open("data/orders.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id","customer_id","product_id","quantity","order_date","status"])
    base = datetime(2024, 1, 1)
    order_id = 1
    for _ in range(500):
        cust = random.randint(1, 200)
        prod = random.randint(1, 12)
        qty  = random.randint(1, 5)
        date = (base + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
        status = random.choice(["completed","completed","completed","cancelled","pending"])
        # inject 5% nulls on quantity
        if random.random() < 0.05: qty = ""
        # inject 5% nulls on customer_id
        if random.random() < 0.05: cust = ""
        w.writerow([order_id, cust, prod, qty, date, status])
        order_id += 1
    # add 10 duplicate rows
    for _ in range(10):
        w.writerow([random.randint(1,500), random.randint(1,200),
                    random.randint(1,12), 1, "2024-06-15", "completed"])

print("Data generated:")
print(f"  customers.csv : 200 rows")
print(f"  products.csv  : 12 rows")
print(f"  orders.csv    : ~510 rows (with nulls + duplicates)")
