# combine all the raw data files
# clean the data
# create/replace a folder named final_data
# save all the files created after cleaning and combining the data into the final_data folder



import pandas as pd
import os

RAW_DIR = "../raw_data"




def combine_files(prefix):
    combined = pd.DataFrame()
    dirlist = os.listdir(RAW_DIR)
    for file in dirlist:
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

# dirlist = os.listdir(RAW_DIR)
# for file in dirlist:
#     print(file)
df = combine_files("sales")

print(df.head(), df.info(), df.shape, df.isnull().sum(), sep="\n\n")



