"""
Prefect Flow: ETL E-Commerce Orders & Products
File ini disesuaikan dari Apache Airflow ke Prefect.
"""
from datetime import datetime, timedelta
from prefect import flow, task
import pandas as pd

# === FUNGSI CALLABLE ===

@task(name="Extract Orders & Products")
def extract_from_source():
    print(f"[{datetime.now()}] [Extract] Mengekstrak data dari raw_orders.csv dan raw_products.csv...")
    df_orders = pd.read_csv("raw_orders.csv")
    df_products = pd.read_csv("raw_products.csv")
    return df_orders, df_products

@task(name="Transform and Clean Data")
def transform_data(data_tuple):
    print(f"[{datetime.now()}] [Transform] Menggabungkan data pesanan dan produk...")
    df_orders, df_products = data_tuple
    
    # Menggabungkan data berdasarkan kolom product_id
    df_merged = pd.merge(df_orders, df_products, on="product_id", how="inner")
    
    # Pembersihan data: Hapus duplikat dan baris kosong
    df_clean = df_merged.dropna().drop_duplicates()
    return df_clean

@task(name="Validate Quality")
def validate_data(df):
    print(f"[{datetime.now()}] [Validate] Memvalidasi kualitas data...")
    if df.empty:
        raise ValueError("Data hasil penggabungan kosong! Kualitas data buruk.")
    print(f"Validation Success: Total {len(df)} baris data valid.")
    return df

@task(name="Load to Warehouse")
def load_to_bigquery(df):
    print(f"[{datetime.now()}] [Load] Memuat data bersih ke BigQuery (Simulasi: CSV)...")
    # DELIVERABLE: Menghasilkan file orders_clean.csv
    df.to_csv("orders_clean.csv", index=False)
    print("Sukses membuat file 'orders_clean.csv'")

@task(name="Generate Report")
def generate_summary(df):
    print(f"[{datetime.now()}] [Report] Membuat ringkasan laporan summary harian...")
    
    # Membuat summary report otomatis
    if 'quantity' in df.columns and 'price' in df.columns and 'product_name' in df.columns:
        df['total_sales'] = df['quantity'] * df['price']
        summary = df.groupby('product_name').agg(
            total_items_sold=('quantity', 'sum'),
            total_revenue=('total_sales', 'sum')
        ).reset_index()
    else:
        summary = df.head(10) 
        
    # DELIVERABLE: Menghasilkan file summary_report.csv
    summary.to_csv("summary_report.csv", index=False)
    print("Sukses membuat file 'summary_report.csv'")

@task(name="Send Notification")
def send_slack_alert():
    print(f"[{datetime.now()}] [Notify] Mengirimkan notifikasi sukses ke Slack...")


# === DEFAULT ARGS ===
default_args = {
    'owner': 'data-engineering-team',
    'retries': 3,                     
    'retry_delay': timedelta(minutes=5),  
}


# === PREFECT FLOW DEFINITION ===
@flow(
    name="etl_ecommerce_daily",
    description='Daily ETL pipeline untuk data transaksi e-commerce',
    retries=default_args['retries'],
    retry_delay_seconds=default_args['retry_delay'].total_seconds(),
)
def etl_ecommerce_daily_flow():
    print(f"[{datetime.now()}] --- Pipeline Dimulai ---")

    data_raw = extract_from_source()
    data_clean = transform_data(data_raw)
    data_valid = validate_data(data_clean)
    
    load_to_bigquery(data_valid)
    generate_summary(data_valid)
    send_slack_alert()

    print(f"[{datetime.now()}] --- Pipeline Senedai ---")


# === DEPLOYMENT RUNNER ===
if __name__ == "__main__":
    etl_ecommerce_daily_flow.serve(
        name="etl-ecommerce-daily-v1",
        cron="0 6 * * *",  
    )