from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Sampler").getOrCreate()

try:
    df = spark.read.option("header", True).csv("./original_dataset/Amazon Sale Report.csv").drop('index').dropDuplicates()
    new_columns = [col.replace(" ", "_").replace("-", "_") for col in df.columns]
    df = df.toDF(*new_columns)

    df.createOrReplaceTempView("tempdata")

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

    from dataframe_corrupter import DataFrameCorrupter
    from dataframe_corrupter import (
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
        (AddDuplicateRowsStrategy(probability=0.2), None),
        (AddNegativeValuesStrategy(probability=0.3), ['Qty', 'Amount']),
        (AddNullsStrategy(probability=0.1), None),
        (AddNoiseStrategy(noise_level=0.1), ['Amount']),
        (AddTyposStrategy(probability=0.1), ['Category']),
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
    