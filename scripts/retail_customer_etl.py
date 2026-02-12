import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error


def transform_customer_data(df):
    ## Standardize gender values
    gender_values = {'Male': 'M', 'male': 'M', 'Female': 'F', 'female': 'F', 'Unknown': '', np.nan: ''}
    df['gender'] = df['gender'].replace(gender_values)

    ## Populate missing age values with the average age
    df['age'] = df['age'].fillna(df['age'].mean()).astype(int)

    ## Parse/standardize signup_date column
    df['signup_date'] = pd.to_datetime(df['signup_date'], format='mixed', errors='coerce').dt.strftime("%Y-%m-%d")

    ## Remove leading & trailing white spaces and capitalize first and last names
    df['first_name'] = df['first_name'].str.strip().str.capitalize()
    df['last_name'] = df['last_name'].str.strip().str.capitalize()

    ## Remove leading & trailing white spaces and capitalize full name
    df['full_name'] = df['full_name'].str.strip().str.title()

    ## Correct misspelled first names
    first_name_cleanup = {'Anbhony': 'Anthony', 'Agthony': 'Anthony', 'Dorothj': 'Dorothy', 'Briln': 'Brian',
                          'Sawdra': 'Sandra', 'Melxssa': 'Melissa', 'Stelen': 'Steven',
                          'Charlfs': 'Charles', 'Jessicv': 'Jessica', 'Chmistopher': 'Christopher',
                          'Garbara': 'Barbara', 'Cetty': 'Betty', 'Pakl': 'Paul', 'Michaew': 'Michael'}
    df['first_name'] = df['first_name'].replace(first_name_cleanup)

    ## Correct misspelled last names
    last_name_cleanup = {'Dasis': 'Davis', 'Moope': 'Moore', 'Taklor': 'Taylor', 'Anberson': 'Anderson',
                         'Write': 'White', 'Taompson': 'Thompson', 'Mooxe': 'Moore',
                         'Andsrson': 'Anderson', 'Johnskn': 'Johnson', 'Czark': 'Clark', 'Thompion': 'Thompson',
                         'Gsrcia': 'Garcia', 'Pvrez': 'Perez', 'Taytor': 'Taylor',
                         'Teylor': 'Taylor', 'Gonzoles': 'Gonzales', 'Rnderson': 'Anderson', 'Nnderson': 'Anderson',
                         'Willihms': 'Williams', 'Sancfez': 'Sanchez',
                         'Gonzaley': 'Gonzales', 'Dhvis': 'Davis', 'Lozez': 'Lopez'}
    df['last_name'] = df['last_name'].replace(last_name_cleanup)

    ## Correct all identified discrepancies between first_name + last_name and full_name
    df['full_name'] = df['full_name'].mask((df['first_name'] + ' ' + df['last_name'] != df['full_name']),
                                           (df['first_name'] + ' ' + df['last_name']))

    ## Remove any leading or trailing whitespace(s) from email addresses
    df['email'] = df['email'].str.strip().str.lower()

    df['email'] = df['email'].str.replace(r'^([^@]+)(gmail|yahoo|outlook|hotmail)\.com$', r'\1@\2.com', regex=True)

    ## Split email addresses into username and provider domain
    df[['eml_pt1', 'eml_pt2']] = df['email'].str.split('@', expand=True)

    ## Correct invalid provider domains for email addresses
    email_domain_corrections = {'hotmailcom': 'hotmail.com', 'gmailcom': 'gmail.com', 'outlookcom': 'outlook.com',
                                'yahoocom': 'yahoo.com'}
    df['eml_pt2'] = df['eml_pt2'].replace(email_domain_corrections, regex=True)

    ## Correct email addresses for any incorrect/mismatched first_name.last_name format
    df['email'] = df['email'].mask(
        ((df['first_name'] + '.' + df['last_name']).str.lower() != df['eml_pt1'].str.strip()),
        ((df['first_name'] + '.' + df['last_name']).str.lower() + '@' + df['eml_pt2']))
    df['email'] = df['email'].fillna('')

    ## Drop temporary/unnecessary columns used for email verification/correction
    df.drop(columns=['eml_pt1', 'eml_pt2'], inplace=True)

    return df


def customer_data_upload(conn, df_cleaned):
    df_sql = df_cleaned.values.tolist()

    cursor = conn.cursor()
    ## Insert query to upload data into table with ON DUPLICATE KEY UPDATE to avoid duplicate entries
    query = """INSERT INTO customers (customer_id, first_name, last_name, full_name, gender, age, email, signup_date) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
    ON DUPLICATE KEY UPDATE 
    customer_id = VALUES(customer_id), 
    first_name = VALUES(first_name), 
    last_name = VALUES(last_name), 
    full_name = VALUES(full_name), 
    gender = VALUES(gender), 
    age = VALUES(age), 
    email = VALUES(email), 
    signup_date = VALUES(signup_date)"""

    for row in df_sql:
        cursor.execute(query, row)

    ## Commit db transactions
    conn.commit()

    ## Close the cursor
    cursor.close()


def db_connect(host_db, user_db, pwd_db, port_db, name_db):
    ## Create DB connection
    try:
        db_conn = mysql.connector.connect(host=host_db, user=user_db, passwd=pwd_db, port=port_db, database=name_db)
        print(f'Connection to {db_name} established')
        return db_conn
    except Error as error:
        print(f'Cannot connect to {db_name} - {error}')
        print(f'Exiting...')
        exit()


def extract_data(file_path):
    df = pd.read_csv(file_path)
    print(df.columns)
    return df


if __name__ == '__main__':
    load_dotenv()  # loads variables from .env into os.environments

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
    filename = 'Customer_Profiles.csv'

    ds_raw_path = os.path.join(home_directory, ds_directory, filename)
    ds_cleaned_path = os.path.join(home_directory, output_directory, "Customer_Profiles_Cleaned.csv")

    ## Establish DB connection
    db = db_connect(db_host, db_user, db_pass, db_port, db_name)

    ## Extract the dataset
    data = extract_data(ds_raw_path)

    ## Transform dataset
    data_cleaned = transform_customer_data(data)

    ## Write transformed/cleanup data to CSV (optional)
    data_cleaned.to_csv(ds_cleaned_path, index=False)

    ## Write transformed/cleanup data to database
    customer_data_upload(db, data_cleaned)

    ## Close DB connection
    db.close()
