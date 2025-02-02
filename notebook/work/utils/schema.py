from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, StructType, StructField

schema_rental = StructType([
    StructField("rental_id", IntegerType(), True),
    StructField("rental_date", LongType(), True),
    StructField("inventory_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("return_date", LongType(), True),
    StructField("staff_id", IntegerType(), True),
    StructField("last_update", LongType(), True),
])

schema_payment = StructType([
    StructField("rental_id", IntegerType(), True),
    StructField("payment_date", LongType(), True),
    StructField("payment_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("staff_id", IntegerType(), True),
    StructField("last_update", LongType(), True),
])