from pyspark.sql import SparkSession

class SparkStreaming():
    @staticmethod
    def get_instance(app_name:str, executor_memory:str="1g", partitions:str="200"):
        spark = (SparkSession.builder
                    .appName(app_name)
                    .master('spark://spark-master:7077')
                    .config("spark.executor.memory", executor_memory)
                    .config("spark.sql.shuffle.partitions", partitions)
                    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                    .config("hive.exec.dynamic.partition.mode", "nonstrict")
                    .config("spark.streaming.backpressure.enabled", "true")
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
                    .config('spark.sql.warehouse.dir', 's3a://lakehouse/')
                    .config('hive.metastore.warehouse.dir', 's3a://lakehouse/')
                    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector:10.0.2')
                    .enableHiveSupport()
                    .getOrCreate())                
        return spark
    
    @staticmethod
    def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
        """
        Creates a kafka read stream

        Parameters:
            spark : SparkSession
                A SparkSession object
            kafka_address: str
                Host address of the kafka bootstrap server
            topic : str
                Name of the kafka topic
            starting_offset: str
                Starting offset configuration, "earliest" by default 
        Returns:
            read_stream: DataStreamReader
        """

        read_stream = (spark.readStream
                            .format("kafka")
                            .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                            .option("failOnDataLoss", False)
                            .option("startingOffsets", starting_offset)
                            .option("subscribe", topic)
                            .load())

        return read_stream
    
    @staticmethod
    def create_file_write_stream(stream, checkpoint_path, storage_path=None, trigger="10 seconds", output_mode="append", file_format="delta", partitions=None):
        """
        Write the stream back to a file store

        Parameters:
            stream : DataStreamReader
                The data stream reader for your stream
            file_format : str
                mongodb, delta, parquet, csv etc
            storage_path : str
                The file output path
            checkpoint_path : str
                The checkpoint location for spark
            trigger : str
                The trigger interval
            output_mode : str
                append, complete, update
        """

        write_stream = (stream
                            .writeStream
                            .format(file_format)
                            .partitionBy(partitions)
                            .option("checkpointLocation", checkpoint_path)
                            .option("path", storage_path)
                            .trigger(processingTime=trigger)
                            .outputMode(output_mode))

        return write_stream
    
    @staticmethod
    def register_delta_table(spark, database, table_name, columns:list, partition:dict):
        column_str = ',\n'.join([f"{col[0]} {col[1]}" for col in columns])
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database}.{table_name} (
                {column_str}
            )
            USING DELTA
            PARTITIONED BY({partition['name']} {partition['type']})
            LOCATION 's3a://lakehouse/{database}.db/{table_name}'
        """)
        # Describe schema
        print(spark.sql(f"DESCRIBED EXTENDED {database}.{table_name}").show())