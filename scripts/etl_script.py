import pandas as pd
from sqlalchemy import create_engine
import os

def run_etl():
    """
    Performs ETL (Extract, Transform, Load) for retail sales data.
    Reads data from CSV, cleans it, and loads it into PostgreSQL.
    This version is adapted for the Kaggle 'Customer Shopping Dataset'
    (mehmettahiraslan/customer-shopping-dataset).
    """
    print("Starting ETL process...")

    # Define paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..')) # Go up one level from 'scripts'
    csv_file_path = os.path.join(project_root, 'data', 'raw', 'sales_data.csv')

    # --- 1. Extract (Read Data) ---
    try:
        # Ensure you've downloaded 'customer_shopping_data.csv' from Kaggle
        # and renamed it to 'sales_data.csv' and placed it in 'data/raw/'
        df = pd.read_csv(csv_file_path)
        print(f"Successfully loaded data from {csv_file_path}. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # --- 2. Transform (Clean and Prepare Data) ---
    print("Performing data transformations...")

    # Convert column names to lowercase and replace spaces with underscores
    # This standardizes names like 'Invoice No' to 'invoice_no', 'Shopping Mall' to 'shopping_mall'
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Rename specific columns for consistency with our desired schema
    df.rename(columns={
        'invoice_no': 'transaction_id',
        'invoice_date': 'date',
        'category': 'product_category',
        'price': 'price_per_unit' # 'price' in dataset is unit price, rename for clarity
    }, inplace=True)

    # Convert 'date' to datetime objects
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Ensure numeric columns are correct and calculate 'total_amount'
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')

    # Calculate 'total_amount' as quantity * price_per_unit
    df['total_amount'] = df['quantity'] * df['price_per_unit']

    # Handle missing values for critical columns after transformations
    df.dropna(subset=['transaction_id', 'date', 'total_amount', 'quantity', 'price_per_unit', 'product_category'], inplace=True)

    # Convert transaction_id to string to avoid potential issues with mixed types if any
    df['transaction_id'] = df['transaction_id'].astype(str)

    # Add a 'total_sales_per_order' column (if not already present or for aggregation example)
    # This is a simple example; more complex transformations might involve aggregations.
    # Ensure 'transaction_id' is suitable for grouping
    df['total_sales_per_order'] = df.groupby('transaction_id')['total_amount'].transform('sum')

    print("Data transformation complete. Sample data after transformation:")
    print(df.head())
    print("\nDataFrame columns after transformation:")
    print(df.columns)

    # --- 3. Load (Write to PostgreSQL) ---
    print("Loading data into PostgreSQL...")

    # Database connection parameters (from Airflow environment variables)
    DB_HOST = os.getenv('DB_HOST', 'retail-db')
    DB_NAME = os.getenv('DB_NAME', 'retail_sales_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')

    try:
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        # Use 'replace' for simplicity; for production, consider 'append' with UPSERT logic
        df.to_sql('sales_data', engine, if_exists='replace', index=False)
        print("Data successfully loaded into 'sales_data' table in PostgreSQL.")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")

if __name__ == "__main__":
    run_etl()