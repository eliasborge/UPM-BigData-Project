from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, isnan, when, count

from data_processor import DataReaderManager, DataProcessor

load_dotenv()

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    data_reader = DataReaderManager.get_data_reader(spark_session=spark_session)
    data = data_reader.read_all_files()
    for line in data.take(10):
        print(line)

    data_processor = DataProcessor(input_data=data)
    data_processor.remove_forbidden_columns()
    data_parsed = data_processor.get_dataframe()
    print(data.printSchema())
    print(data_parsed.printSchema())

 # Cast string columns to numeric types
    numeric_cols = ['DepTime', 'CRSDepTime', 'CRSArrTime', 'FlightNum', 'CRSElapsedTime', 
                    'DepDelay', 'Distance', 'Cancelled', 'ArrDelay']
    for col_name in numeric_cols:
        data_parsed = data_parsed.withColumn(col_name, col(col_name).cast("integer"))


    imputer = Imputer(inputCols=numeric_cols, outputCols=numeric_cols).setStrategy("mean")
    data_parsed = imputer.fit(data_parsed).transform(data_parsed)


   
    # Index categorical columns
    categorical_cols = ['UniqueCarrier', 'TailNum', 'Origin', 'Dest', 'CancellationCode']
    indexers = [StringIndexer(inputCol=col, outputCol=col+"_index").fit(data_parsed) for col in categorical_cols]

    # Apply indexers to the DataFrame
    for indexer in indexers:
        data_parsed = indexer.transform(data_parsed)

    # Define feature columns excluding 'ArrDelay' and original categorical columns
    feature_columns = [
        'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime',
        'CRSArrTime', 'FlightNum', 'CRSElapsedTime', 'DepDelay', 'Distance', 'Cancelled', 'UniqueCarrier_index', 'TailNum_index', 
        'Origin_index', 'Dest_index', 'CancellationCode_index'
    ]

    # Assemble features into a vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="keep")
    vectorized_df = assembler.transform(data_parsed)

    # Split the data into training and test sets
    train_data, test_data = vectorized_df.randomSplit([0.8, 0.2], seed=42)

    # Check for nulls in the label column again after splitting
    if train_data.filter(col("ArrDelay").isNull()).count() > 0 or train_data.filter(isnan(col("ArrDelay"))).count() > 0:
        raise ValueError("The label column still contains null or NaN values after preprocessing.")

    if train_data.count() > 0:
        lr = LinearRegression(featuresCol="features", labelCol="ArrDelay")
        lr_model = lr.fit(train_data)

        # Make predictions
        predictions = lr_model.transform(test_data)
        # ... [The rest of your model prediction and evaluation code] ...
    else:
        print("No valid data to train on.")

    # Make predictions
    predictions = lr_model.transform(test_data)

    # Show predictions
    predictions.select("prediction", "ArrDelay", "features").show()

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data =", rmse)

    # Stop the SparkSession
    spark_session.stop()
