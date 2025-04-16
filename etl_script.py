import os
import pandas as pd

RAW_DIR = "raw_data"
FINAL_DIR = "final_data"
os.makedirs(FINAL_DIR, exist_ok=True)

def combine_files(prefix):
    combined = pd.DataFrame()
    for file in os.listdir(RAW_DIR):
        if file.startswith(prefix) and file.endswith(".csv"):
            df = pd.read_csv(os.path.join(RAW_DIR, file))
            combined = pd.concat([combined, df], ignore_index=True)
    return combined

def save_deduplicated(df_new, path, unique_cols):
    if os.path.exists(path):
        df_existing = pd.read_csv(path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=unique_cols, keep="last", inplace=True)
    else:
        df_combined = df_new.drop_duplicates(subset=unique_cols, keep="last")
    df_combined.to_csv(path, index=False)
    print(f"✅ Saved and deduplicated: {os.path.basename(path)}")

# SALES
def clean_sales():
    print("\n📄 Cleaning SALES data...")
    df = combine_files("sales")
    if df.empty:
        print("❌ No sales files found.")
        return

    print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')

    df.dropna(subset=['store_id', 'product_id', 'customer_id', 'sale_date'], inplace=True)

    df['quantity'].fillna(1, inplace=True)
    df['quantity'] = df['quantity'].astype(int)
    df['discount'].fillna(0, inplace=True)
    df['price'].fillna(df['price'].mean(), inplace=True)

    df['total_amount'] = (df['quantity'] * df['price']) - df['discount']
    df = df[df['total_amount'] >= 0]

    save_deduplicated(df, os.path.join(FINAL_DIR, "final_sales.csv"),
                      ['store_id', 'product_id', 'customer_id', 'sale_date'])

# CUSTOMERS
def clean_customers():
    print("\n👥 Cleaning CUSTOMERS data...")
    df = combine_files("customers")
    if df.empty:
        print("❌ No customer files found.")
        return

    print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")

    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    df.loc[(df['age'] < 15) | (df['age'] > 100), 'age'] = None
    df['age'].fillna(df['age'].median(), inplace=True)

    df['gender'].fillna("Other", inplace=True)
    df['loyalty_status'].fillna("Unknown", inplace=True)
    df.dropna(subset=['customer_id'], inplace=True)

    save_deduplicated(df, os.path.join(FINAL_DIR, "final_customers.csv"), ["customer_id"])

# INVENTORY
def clean_inventory():
    print("\n📦 Cleaning INVENTORY data...")
    df = combine_files("inventory")
    if df.empty:
        print("❌ No inventory files found.")
        return

    print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")

    df['stock_on_hand'] = pd.to_numeric(df['stock_on_hand'], errors='coerce')
    df['stock_sold'] = pd.to_numeric(df['stock_sold'], errors='coerce')
    df['reorder_level'] = pd.to_numeric(df['reorder_level'], errors='coerce')

    df.fillna(0, inplace=True)
    df['stock_on_hand'] = df['stock_on_hand'].apply(lambda x: max(0, x))
    df['stock_sold'] = df['stock_sold'].apply(lambda x: max(0, x))
    df['reorder_level'] = df['reorder_level'].apply(lambda x: max(0, x))

    df.to_csv(os.path.join(FINAL_DIR, "final_inventory.csv"), index=False)
    print("✅ Saved final_inventory.csv")

# PRODUCTS
def clean_products():
    print("\n🛒 Cleaning PRODUCTS data...")
    path = os.path.join(RAW_DIR, "products.csv")
    if not os.path.exists(path):
        print("❌ products file not found.")
        return

    df = pd.read_csv(path)

    print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")

    df.dropna(subset=['product_id', 'product_name', 'price'], inplace=True)
    df.to_csv(os.path.join(FINAL_DIR, "final_products.csv"), index=False)
    print("✅ Saved final_products.csv")

# STORES
def clean_stores():
    print("\n🏬 Cleaning STORES data...")
    path = os.path.join(RAW_DIR, "stores.csv")
    if not os.path.exists(path):
        print("❌ stores file not found.")
        return

    df = pd.read_csv(path)

    print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")

    df.dropna(subset=['store_id', 'store_name'], inplace=True)
    df.to_csv(os.path.join(FINAL_DIR, "final_stores.csv"), index=False)
    print("✅ Saved final_stores.csv")

def run_etl():
    print("\n🚀 Running ETL process with profiling...\n")
    clean_sales()
    clean_customers()
    clean_inventory()
    clean_products()
    clean_stores()
    print("\n🎉 ETL complete! Cleaned files are ready in final_data/\n")

if __name__ == "__main__":
    run_etl()

