import pandas as pd

     # Read the CSV file
     df = pd.read_csv('bank_transactions.csv')

     # Print column names
     print("Columns in the CSV:")
     print(df.columns.tolist())

     # Print column names and their data types
     print("\nColumn Names and Data Types:")
     print(df.dtypes)
