import pandas as pd
import numpy as np
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

rng = np.random.default_rng(42)


# ============================================================
# OUTPUT FOLDER
# ============================================================

out = Path.cwd() / "customer_order_dataset"
out.mkdir(parents=True, exist_ok=True)
out = out.resolve()

print("=" * 70)
print("DATA GENERATION STARTED")
print("=" * 70)

print("\nCurrent working directory:")
print(Path.cwd().resolve())

print("\nOutput folder:")
print(out)

print("\n" + "=" * 70)


# ============================================================
# 1. CUSTOMERS DATA
# ============================================================

print("Generating customers data...")

n_customers = 1000

cities_states = [
    ("Hyderabad", "Telangana"),
    ("Bengaluru", "Karnataka"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Chennai", "Tamil Nadu"),
    ("Delhi", "Delhi"),
    ("Noida", "Uttar Pradesh"),
    ("Gurugram", "Haryana"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Indore", "Madhya Pradesh"),
    ("Nagpur", "Maharashtra"),
    ("Lucknow", "Uttar Pradesh"),
    ("Bhopal", "Madhya Pradesh"),
    ("Kochi", "Kerala"),
    ("Surat", "Gujarat"),
    ("Patna", "Bihar"),
    ("Bhubaneswar", "Odisha"),
    ("Visakhapatnam", "Andhra Pradesh")
]

segments = [
    "Consumer",
    "Corporate",
    "SMB",
    "Premium"
]

customers = pd.DataFrame({
    "customer_id": np.arange(
        1,
        n_customers + 1
    ),

    "customer_name": [
        f"Customer_{i:04d}"
        for i in range(
            1,
            n_customers + 1
        )
    ],

    "email": [
        f"customer{i:04d}@example.com"
        for i in range(
            1,
            n_customers + 1
        )
    ]
})


# Generate city and state
locs = [
    cities_states[i]
    for i in rng.integers(
        0,
        len(cities_states),
        n_customers
    )
]

customers["city"] = [
    x[0]
    for x in locs
]

customers["state"] = [
    x[1]
    for x in locs
]


# Generate signup dates
customers["signup_date"] = pd.to_datetime(
    rng.integers(
        pd.Timestamp("2022-01-01").value // 10**9,
        pd.Timestamp("2026-08-31").value // 10**9,
        n_customers
    ),
    unit="s"
).date


# Generate customer segments
customers["customer_segment"] = rng.choice(
    segments,
    n_customers,
    p=[
        0.48,
        0.20,
        0.22,
        0.10
    ]
)


# Arrange customer columns
customers = customers[
    [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "signup_date",
        "customer_segment"
    ]
]


# Save customers
customers_file = out / "customers.csv"

customers.to_csv(
    customers_file,
    index=False
)

print(
    f"Customers generated: "
    f"{len(customers):,}"
)

print(
    f"File saved: "
    f"{customers_file.resolve()}"
)

print()


# ============================================================
# 2. PRODUCTS DATA
# ============================================================

print("Generating products data...")

n_products = 200

categories = {
    "Electronics": (
        800,
        80000
    ),

    "Home Appliances": (
        1200,
        65000
    ),

    "Furniture": (
        2500,
        90000
    ),

    "Mobile Accessories": (
        150,
        5000
    ),

    "Office Supplies": (
        50,
        3000
    ),

    "Grocery": (
        30,
        2500
    ),

    "Fashion": (
        300,
        12000
    ),

    "Sports": (
        250,
        15000
    )
}


suppliers = [
    "ABC Traders",
    "Global Supply Co",
    "Prime Distributors",
    "Metro Wholesale",
    "National Suppliers",
    "Smart Retail Supply"
]


# Assign product categories
product_categories = rng.choice(
    list(categories.keys()),
    n_products
)


# Create products
products = pd.DataFrame({
    "product_id": np.arange(
        1,
        n_products + 1
    ),

    "product_name": [
        f"Product_{i:04d}"
        for i in range(
            1,
            n_products + 1
        )
    ],

    "category": product_categories,

    "supplier": rng.choice(
        suppliers,
        n_products
    )
})


# Generate product prices
prices = []

for category in product_categories:

    min_price, max_price = categories[category]

    price = round(
        rng.uniform(
            min_price,
            max_price
        ),
        2
    )

    prices.append(price)


products["price"] = prices


# Arrange product columns
products = products[
    [
        "product_id",
        "product_name",
        "category",
        "price",
        "supplier"
    ]
]


# Save products
products_file = out / "products.csv"

products.to_csv(
    products_file,
    index=False
)

print(
    f"Products generated: "
    f"{len(products):,}"
)

print(
    f"File saved: "
    f"{products_file.resolve()}"
)

print()


# ============================================================
# 3. ORDERS DATA
# ============================================================

print("Generating orders data...")

n_orders = 50000


# ------------------------------------------------------------
# Generate customer IDs
# ------------------------------------------------------------

customer_ids = rng.integers(
    1,
    n_customers + 1,
    n_orders
)


# ------------------------------------------------------------
# Generate product IDs
# ------------------------------------------------------------

product_ids = rng.integers(
    1,
    n_products + 1,
    n_orders
)


# ------------------------------------------------------------
# Generate order dates
# ------------------------------------------------------------

order_dates = pd.to_datetime(
    rng.integers(
        pd.Timestamp("2025-01-01").value // 10**9,
        pd.Timestamp("2026-08-31").value // 10**9,
        n_orders
    ),
    unit="s"
).date


# ------------------------------------------------------------
# Product price lookup
# ------------------------------------------------------------

product_price_lookup = products.set_index(
    "product_id"
)["price"]


base_prices = product_price_lookup.loc[
    product_ids
].to_numpy()


# ------------------------------------------------------------
# Generate selling prices
# ------------------------------------------------------------

unit_prices = np.round(
    base_prices *
    rng.uniform(
        0.85,
        1.05,
        n_orders
    ),
    2
)


# ============================================================
# CREATE ORDERS
# ============================================================

orders = pd.DataFrame({

    "order_id": np.arange(
        1,
        n_orders + 1
    ),

    "customer_id": customer_ids,

    "product_id": product_ids,

    "order_date": order_dates,

    "quantity": rng.choice(
        [
            1,
            2,
            3,
            4,
            5,
            6,
            8,
            10
        ],
        n_orders,
        p=[
            0.30,
            0.28,
            0.16,
            0.10,
            0.06,
            0.04,
            0.03,
            0.03
        ]
    ),

    "unit_price": unit_prices,

    "payment_method": rng.choice(
        [
            "UPI",
            "Credit Card",
            "Debit Card",
            "Net Banking",
            "Cash on Delivery",
            "Wallet"
        ],
        n_orders,
        p=[
            0.34,
            0.22,
            0.15,
            0.12,
            0.09,
            0.08
        ]
    ),

    "order_status": rng.choice(
        [
            "Delivered",
            "Shipped",
            "Processing",
            "Cancelled",
            "Returned"
        ],
        n_orders,
        p=[
            0.65,
            0.12,
            0.10,
            0.07,
            0.06
        ]
    ),

    "store_id": rng.integers(
        1,
        51,
        n_orders
    )
})


# ============================================================
# ADD CUSTOMER NAME TO ORDERS
# ============================================================

orders = orders.merge(
    customers[
        [
            "customer_id",
            "customer_name"
        ]
    ],
    on="customer_id",
    how="left"
)


# ============================================================
# ADD PRODUCT NAME TO ORDERS
# ============================================================

orders = orders.merge(
    products[
        [
            "product_id",
            "product_name"
        ]
    ],
    on="product_id",
    how="left"
)


# ============================================================
# ARRANGE ORDER COLUMNS
# ============================================================

orders = orders[
    [
        "order_id",
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "order_date",
        "quantity",
        "unit_price",
        "payment_method",
        "order_status",
        "store_id"
    ]
]


# ============================================================
# SHUFFLE ORDERS
# ============================================================

orders = (
    orders
    .sample(
        frac=1,
        random_state=42
    )
    .reset_index(
        drop=True
    )
)


# ============================================================
# SAVE ORDERS
# ============================================================

orders_file = out / "orders.csv"

orders.to_csv(
    orders_file,
    index=False
)

print(
    f"Orders generated: "
    f"{len(orders):,}"
)

print(
    f"File saved: "
    f"{orders_file.resolve()}"
)

print()


# ============================================================
# 4. CREATE ENRICHED ORDERS
# ============================================================

print("Creating enriched orders dataset...")


# Add complete customer information
orders_enriched = orders.merge(
    customers[
        [
            "customer_id",
            "email",
            "customer_segment",
            "city",
            "state"
        ]
    ],
    on="customer_id",
    how="left"
)


# Add complete product information
orders_enriched = orders_enriched.merge(
    products[
        [
            "product_id",
            "category",
            "supplier",
            "price"
        ]
    ],
    on="product_id",
    how="left"
)


# Calculate total amount
orders_enriched["total_amount"] = np.round(
    orders_enriched["quantity"] *
    orders_enriched["unit_price"],
    2
)


# Arrange final columns
orders_enriched = orders_enriched[
    [
        "order_id",

        # Customer information
        "customer_id",
        "customer_name",
        "email",
        "customer_segment",
        "city",
        "state",

        # Product information
        "product_id",
        "product_name",
        "category",
        "supplier",
        "price",

        # Order information
        "order_date",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        "order_status",
        "store_id"
    ]
]


# ============================================================
# SAVE ENRICHED ORDERS
# ============================================================

enriched_file = out / "orders_enriched.csv"

orders_enriched.to_csv(
    enriched_file,
    index=False
)

print(
    f"Enriched orders generated: "
    f"{len(orders_enriched):,}"
)

print(
    f"File saved: "
    f"{enriched_file.resolve()}"
)

print()


# ============================================================
# 5. DATA VALIDATION
# ============================================================

print("=" * 70)
print("DATA VALIDATION")
print("=" * 70)


# Validate customer IDs
invalid_customer_ids = (
    ~orders["customer_id"].isin(
        customers["customer_id"]
    )
).sum()


# Validate product IDs
invalid_product_ids = (
    ~orders["product_id"].isin(
        products["product_id"]
    )
).sum()


# Validate duplicate order IDs
duplicate_order_ids = (
    orders["order_id"].duplicated()
).sum()


# Validate missing customer names
missing_customer_names = (
    orders["customer_name"].isna()
).sum()


# Validate missing product names
missing_product_names = (
    orders["product_name"].isna()
).sum()


# Validate missing customer IDs
missing_customer_ids = (
    orders["customer_id"].isna()
).sum()


# Validate missing product IDs
missing_product_ids = (
    orders["product_id"].isna()
).sum()


print()

print(
    f"Invalid customer IDs    : "
    f"{invalid_customer_ids:,}"
)

print(
    f"Invalid product IDs     : "
    f"{invalid_product_ids:,}"
)

print(
    f"Duplicate order IDs     : "
    f"{duplicate_order_ids:,}"
)

print(
    f"Missing customer IDs    : "
    f"{missing_customer_ids:,}"
)

print(
    f"Missing product IDs     : "
    f"{missing_product_ids:,}"
)

print(
    f"Missing customer names  : "
    f"{missing_customer_names:,}"
)

print(
    f"Missing product names   : "
    f"{missing_product_names:,}"
)

print()


# ============================================================
# 6. DATASET SUMMARY
# ============================================================

print("=" * 70)
print("DATASET SUMMARY")
print("=" * 70)


files = [
    customers_file,
    products_file,
    orders_file,
    enriched_file
]


summary = pd.DataFrame({

    "File": [
        file.name
        for file in files
    ],

    "Records": [
        len(customers),
        len(products),
        len(orders),
        len(orders_enriched)
    ],

    "Size_MB": [
        round(
            file.stat().st_size / 1024**2,
            2
        )
        for file in files
    ]
})


print()

print(
    summary.to_string(
        index=False
    )
)


# ============================================================
# 7. BUSINESS STATISTICS
# ============================================================

print()

print("=" * 70)
print("BUSINESS STATISTICS")
print("=" * 70)


total_revenue = (
    orders_enriched["total_amount"]
    .sum()
)


average_order_value = (
    orders_enriched["total_amount"]
    .mean()
)


print()

print(
    f"Total Orders        : "
    f"{len(orders):,}"
)

print(
    f"Total Customers     : "
    f"{len(customers):,}"
)

print(
    f"Total Products      : "
    f"{len(products):,}"
)

print(
    f"Total Revenue       : "
    f"₹{total_revenue:,.2f}"
)

print(
    f"Average Order Value : "
    f"₹{average_order_value:,.2f}"
)


# ============================================================
# 8. ORDER STATUS SUMMARY
# ============================================================

print()

print("=" * 70)
print("ORDER STATUS SUMMARY")
print("=" * 70)


status_summary = (
    orders["order_status"]
    .value_counts()
    .reset_index()
)


status_summary.columns = [
    "order_status",
    "order_count"
]


print()

print(
    status_summary.to_string(
        index=False
    )
)


# ============================================================
# 9. PAYMENT METHOD SUMMARY
# ============================================================

print()

print("=" * 70)
print("PAYMENT METHOD SUMMARY")
print("=" * 70)


payment_summary = (
    orders["payment_method"]
    .value_counts()
    .reset_index()
)


payment_summary.columns = [
    "payment_method",
    "order_count"
]


print()

print(
    payment_summary.to_string(
        index=False
    )
)


# ============================================================
# 10. SHOW SAMPLE DATA
# ============================================================

print()

print("=" * 70)
print("CUSTOMER SAMPLE")
print("=" * 70)

print()

print(
    customers.head(5).to_string(
        index=False
    )
)


print()

print("=" * 70)
print("PRODUCT SAMPLE")
print("=" * 70)

print()

print(
    products.head(5).to_string(
        index=False
    )
)


print()

print("=" * 70)
print("ORDERS SAMPLE")
print("=" * 70)

print()

print(
    orders.head(5).to_string(
        index=False
    )
)


print()

print("=" * 70)
print("ENRICHED ORDERS SAMPLE")
print("=" * 70)

print()

print(
    orders_enriched.head(5).to_string(
        index=False
    )
)


# ============================================================
# 11. VERIFY GENERATED FILES
# ============================================================

print()

print("=" * 70)
print("VERIFYING GENERATED FILES")
print("=" * 70)


all_files_created = True


for file in files:

    print()

    if file.exists():

        size_mb = (
            file.stat().st_size /
            1024**2
        )

        print("✓ FILE CREATED SUCCESSFULLY")

        print(
            f"  Name : "
            f"{file.name}"
        )

        print(
            f"  Path : "
            f"{file.resolve()}"
        )

        print(
            f"  Size : "
            f"{size_mb:.2f} MB"
        )

    else:

        all_files_created = False

        print("✗ FILE NOT FOUND")

        print(
            f"  Expected path: "
            f"{file.resolve()}"
        )


# ============================================================
# 12. FINAL OUTPUT
# ============================================================

print()

print("=" * 70)
print("DATA GENERATION COMPLETED")
print("=" * 70)

print()


if all_files_created:

    print(
        "SUCCESS: All files were created successfully."
    )

else:

    print(
        "WARNING: One or more files were not created."
    )


print()

print("=" * 70)
print("YOUR DATASET FOLDER")
print("=" * 70)

print()

print(
    out.resolve()
)


print()

print("Generated files:")

print()

for file in files:

    print(
        f"  ├── {file.name}"
    )


print()

print("=" * 70)
print("RECORD COUNTS")
print("=" * 70)

print()

print(
    f"Customers       : "
    f"{len(customers):,}"
)

print(
    f"Products        : "
    f"{len(products):,}"
)

print(
    f"Orders          : "
    f"{len(orders):,}"
)

print(
    f"Orders Enriched : "
    f"{len(orders_enriched):,}"
)


print()

print("=" * 70)
print("OPEN THIS EXACT FOLDER")
print("=" * 70)

print()

print(
    out.resolve()
)

print()

print("DONE!")