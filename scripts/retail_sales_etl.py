import os
from dotenv import load_dotenv
import pandas as pd
import mysql.connector
from mysql.connector import Error
from thefuzz import process


def get_match(row):
        product_corrections = ['Coffee Beans 1Kg', 'Mechanical Keyboard', 'Office Chair', 'Standing Desk', 'USB-C Cable', 'Highlighter Set', 'Notebook A5', 
                               'Ballpoint Pen', 'Green Tea Box', 'Wireless Mouse']
            
        # Returns (best_match, score)
        match, score = process.extractOne(row, product_corrections)
        return match if score > 85 else row


def retail_data_cleanup(df, prod_cleaned_path, cat_cleaned_path, rs_cleaned_path):
    # Remove all non-digit characters from the beginning of the 'transactions_id' column
    df['transaction_id'] = df['transaction_id'].str.replace(r'^\D+', '', regex=True)

    ## Remove all leading and trailing whitespace from 'sale_date' column
    df['sale_date'] = df['sale_date'].str.strip()

    ## Parse/standardize sale_date column
    df['sale_date'] = pd.to_datetime(df['sale_date'], format='mixed', errors='coerce').dt.strftime("%Y-%m-%d")

    ## Remove all leading and trailing whitespace from 'sale_time' column
    df['sale_time'] = df['sale_time'].str.strip()

    df['sale_time'] = df['sale_time'].str.replace(r'[^\d]', ':', regex=True)

    ## Parse/standardize time column
    df['sale_time'] = pd.to_datetime(df['sale_time'], format='mixed', errors='coerce').dt.strftime('%H:%M:%S')

    ## Remove all leading and trailing whitespace from 'category' and capitalize each word
    df['category'] = df['category'].str.strip().str.title()

    ## Correct/Fix category names   
    df['category'] = df['category'].replace('.*(Furni|ture).*', 'Furniture', regex=True)
    df['category'] = df['category'].replace('.*(Elec|Electr|tronics).*', 'Electronics', regex=True)
    df['category'] = df['category'].replace('.*(Stati|onery).*', 'Stationery', regex=True)
    df['category'] = df['category'].replace('.*(Gro|cery).*', 'Grocery', regex=True)

    # Create a mapping of ID to Category & vice versa
    mapping_id = {1: 'Electronics', 2: 'Furniture', 3: 'Stationery', 4: 'Grocery'}
    mapping_cat = {'Electronics': 1, 'Furniture': 2, 'Stationery': 3, 'Grocery': 4}

    # Map the IDs to the category column & populate the NaN values
    df['category'] = df['category'].fillna(df['category_id'].map(mapping_id))

    # Map the Category to the ID column & populate the NaN values
    df['category_id'] = df['category_id'].fillna(df['category'].map(mapping_cat))

    df['category_id'] = df['category_id'].fillna(0)

    # Convert Category ID column from float to integer
    df['category_id'] = df['category_id'].astype(int)

    # Create category dataframe with only the ID and Category columns, dropping NaNs and duplicates
    df_category = df[['category_id', 'category']].dropna().drop_duplicates().reset_index(drop=True)

    df_category.to_csv(cat_cleaned_path, index=False)

    ## Remove all leading and trailing whitespace from 'product' and capitalize each word
    df['product'] = df['product'].str.strip().str.title()

    ## Fill missing product values with 'Unknown'
    df['product'] = df['product'].fillna('Unknown')

    ## Correct/Fix product names
    df['product'] = df['product'].apply(get_match)

    # Create Product to ID mapping
    prod_id_map = {'Coffee Beans 1Kg': 401, 'Mechanical Keyboard': 102, 'Standing Desk': 202, 'USB-C Cable': 103, 'Highlighter Set': 303, 'Office Chair': 201, 'Ballpoint Pen': 302, 
                'Green Tea Box': 402, 'Wireless Mouse': 101, 'Notebook A5': 301}

    # Map the product to the product ID & replace/populate the NaN values in the product ID column
    df['product_id'] = df['product_id'].fillna(df['product'].map(prod_id_map))

    # Convert the Product ID column from float to integer
    df['product_id'] = df['product_id'].astype(int)

    # Create dataframe of product IDs and names
    df_product = df[['product_id', 'product']].dropna().drop_duplicates().reset_index(drop=True)

    # Drop all rows which contain 'Unknown' in the product name
    df_product = df_product[~df_product['product'].str.contains('Unknown', na=False)]

    df_product.to_csv(prod_cleaned_path, index=False)

    # Drop rows where 'quantity' and 'total_sale' columns are both NaN/null.
    df = df.dropna(subset=['quantity', 'total_sale'], how='all')

    # Convert the Quantity values from float to integer
    df['quantity'] = df['quantity'].astype(int) 

    df['total_sale'] = df['total_sale'].fillna(df['quantity'] * df['price_per_unit']).round(2)

    df.drop(columns=['product', 'category'], inplace=True)

    ## Create CSV file with clean dataset
    df.to_csv(rs_cleaned_path, index=False)

    return df


def product_data_upload(db, prod_cleaned_path):
    df = pd.read_csv(prod_cleaned_path)
    df_sql = df.values.tolist()

    cursor = db.cursor()
    query = """INSERT INTO products (product_id, product_name) VALUES (%s, %s) 
    ON DUPLICATE KEY UPDATE 
    product_id = VALUES(product_id), 
    product_name = VALUES(product_name)"""

    for row in df_sql:
        cursor.execute(query, row)

    db.commit()
    cursor.close()


def category_data_upload(db, cat_cleaned_path):
    df = pd.read_csv(cat_cleaned_path)
    df_sql = df.values.tolist()

    cursor = db.cursor()
    query = """INSERT INTO category (category_id, category_name) VALUES (%s, %s) 
    ON DUPLICATE KEY UPDATE 
    category_id = VALUES(category_id), 
    category_name = VALUES(category_name)"""

    for row in df_sql:
        cursor.execute(query, row)

    db.commit()
    cursor.close()


def retail_data_upload(db, df_cleaned):
    df_sql = df_cleaned.values.tolist()

    cursor = db.cursor()
    query = """INSERT INTO sales (transaction_id, sale_date, sale_time, customer_id, product_id, category_id, 
                quantity, unit_price, total_sale) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON DUPLICATE KEY UPDATE 
    transaction_id = VALUES(transaction_id), 
    sale_date = VALUES(sale_date), 
    sale_time = VALUES(sale_time), 
    customer_id = VALUES(customer_id), 
    product_id = VALUES(product_id), 
    category_id = VALUES(category_id), 
    quantity = VALUES(quantity), 
    unit_price = VALUES(unit_price), 
    total_sale = VALUES(total_sale)"""

    for row in df_sql:
        cursor.execute(query, row)

    db.commit()
    cursor.close()


def db_connect(db_host, db_user, db_pass, db_port, db_name):
    ## Open DB connection
    try:
        db = mysql.connector.connect(host=db_host, user=db_user, passwd=db_pass, port=db_port, database=db_name)
        print(f'Connection to {db_name} established')
        return db
    except Error as error:
        print(f'Cannot connect to {db_name} - {error}')
        print(f'Exiting...')
        exit()
    

def etl_sales_main():
    load_dotenv() # loads variables from .env into os.environments

    ## --- DB Parameters ---
    db_host = os.getenv("MYSQL_HOST")
    db_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB")
    db_user = os.getenv("MYSQL_USER")
    db_pass = os.getenv("MYSQL_PWD")

    ## --- Directory Parameters ---
    home_directory = os.getenv("HOME_DIR")
    ds_directory = os.getenv("DATASET_DIR")
    output_directory = os.getenv("OUTPUT_DIR")
    filename = 'Sales_Data_2023_2025.csv'

    ds_raw_path = os.path.join(home_directory, ds_directory, filename)

    rs_cleaned_path = os.path.join(home_directory, output_directory, "Retail_Sales_Cleaned.csv")
    cat_cleaned_path = os.path.join(home_directory, output_directory, "Category_Cleaned.csv")
    prod_cleaned_path = os.path.join(home_directory, output_directory, "Product_Cleaned.csv")

    ## Establish DB connection
    db = db_connect(db_host, db_user, db_pass, db_port, db_name)

    ## Load the dataset
    df = pd.read_csv(ds_raw_path)
    
    df_cleaned = retail_data_cleanup(df, prod_cleaned_path, cat_cleaned_path, rs_cleaned_path)

    product_data_upload(db, prod_cleaned_path)
    category_data_upload(db, cat_cleaned_path)
    retail_data_upload(db, df_cleaned)

    ## Close DB connection
    db.close()


if __name__ == '__main__':
    etl_sales_main()
