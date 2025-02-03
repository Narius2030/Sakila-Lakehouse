import sys
sys.path.append("./notebook/work")

import csv
import time
import logging
import polars as pl
from operators.database_operators import SQLOperators
from utils.config import get_settings

settings = get_settings()
dbopt = SQLOperators("sakila", settings)

def read_csv(filename:str):
    try:
        with open(f"./notebook/data/{filename}", encoding="UTF-8") as file:
            reader = csv.DictReader(file)
            data = list(reader)
            return data, reader.fieldnames
    except FileNotFoundError:
        logging.error(f"Error: File '{filename}' not found.")
        return None, None
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        return None, None

def data_generator(dataset, key):
    for row in dataset:
        print(row)
        yield {'value': row, 'key': {key: row[key]}}
        time.sleep(2)

def read_rental_payment():
    rental_dataset, rental_columns = read_csv("rental.csv")
    payment_dataset, payment_columns = read_csv("payment.csv")
    
    rental_df = pl.DataFrame(rental_dataset, infer_schema_length=1000)
    payment_df = pl.DataFrame(payment_dataset, infer_schema_length=1000).drop("last_update")
    joined_df = rental_df.join(other=payment_df, on="rental_id", how="inner")
    
    rental_datasets = joined_df.select(rental_columns).sort("rental_id", descending=False).to_dicts()
    payment_datasets = joined_df.select(payment_columns).sort("rental_id", descending=False).to_dicts()
    
    
    for rental, payment in zip(rental_datasets, payment_datasets):
        try:
            dbopt.insert_dataframe_table_nonconflict("rental", "public", [rental], rental_df.columns, ("rental_id",), chunk_size=100)
            dbopt.insert_dataframe_table_nonconflict("payment", "public", [payment], payment_df.columns, ("payment_id",), chunk_size=100)
            print("Inserted Successfully")
        except Exception as ex:
            logging.error(f"An error occurred: {str(ex)}")
            
        
if __name__ == '__main__':
    read_rental_payment()