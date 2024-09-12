from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType
import os

spark = SparkSession.builder.appName("Sampler").getOrCreate()

try:
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv("./original_dataset/Amazon Sale Report.csv").drop('index').dropDuplicates()
        
    new_columns = [col.replace(" ", "_").replace("-", "_") for col in df.columns]
    df = df.toDF(*new_columns)

    # extracting distinct values
    df.createOrReplaceTempView("tempdata")
    
    # critical columns example
    # the pipeline validates it's spelling
    # add more if needed, it's just an example
    columnList = ["Status", "Fulfilment", "ORDERS_Channel", "Category"]
    
    for col in columnList:
        if col not in ['Order_ID', 'Date']:
            df_dist=spark.sql(f"SELECT DISTINCT {col} FROM tempdata")
            df_dist.coalesce(1).write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save(f'./unique_keys/{col}.csv')

    sampling_query=f"""
    WITH RANDOM_ AS (
        SELECT *, RAND() AS rndm
        FROM tempdata
    )
    SELECT {', '.join(df.columns)}
    FROM RANDOM_
    WHERE rndm > 0.6
    LIMIT 25
    """

    sample1 = spark.sql(sampling_query)
    sample2 = spark.sql(sampling_query)

    # Save the sampled data as a CSV file
    sample1.write.csv("./batchs/sampled_data_1.csv", header=True)

    # IMPORTANT NOTE
    # I HAVE NOT UPLOADED dataframe_corrupter TO PYPI YET!
    from dataframe_corrupter import DataFrameCorrupter # type: ignore
    from dataframe_corrupter import ( # type: ignore
        AddDuplicateRowsStrategy,
        AddNegativeValuesStrategy,
        AddNullsStrategy,
        AddNoiseStrategy,
        AddTyposStrategy,
    )

    pd_sample2 = sample2.toPandas()
    # cast amount to float
    pd_sample2['Amount'] = pd_sample2['Amount'].astype(float, errors='ignore')

    corruption_strategies=[
        (AddDuplicateRowsStrategy(probability=0.05), None),
        (AddNegativeValuesStrategy(probability=0.05), ['Qty', 'Amount']),
        (AddNullsStrategy(probability=0.01), None),
        (AddNoiseStrategy(noise_level=0.05), ['Amount']),
        (AddTyposStrategy(probability=0.003), ['Category']),
    ]
    corrupter = DataFrameCorrupter(corruption_strategies)

    df_corr = corrupter.corrupt(pd_sample2)

    os.makedirs('./batchs/sampled_data_2.csv/', exist_ok=True)
    df_corr.to_csv('./batchs/sampled_data_2.csv/sampled_data_2.csv', index=False)

    spark.stop()
except Exception as e:
    print(e)
    raise
finally:
    spark.stop()
    