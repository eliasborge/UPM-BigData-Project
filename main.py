from dotenv import load_dotenv
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer, UnivariateFeatureSelector
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan

from data_processor import DataReaderManager, DataProcessor

load_dotenv()


if __name__ == "__main__":
	spark_session = SparkSession.builder.getOrCreate()
	data_reader = DataReaderManager.get_data_reader(spark_session=spark_session)
	data = data_reader.read_all_files()

	data_processor = DataProcessor(input_data=data)
	data_parsed = data_processor.get_processed()

	# Cast string columns to numeric types
	numeric_cols = ['DepTime_timestamp', 'CRSDepTime_timestamp', 'CRSArrTime_timestamp', 'FlightNum', 'CRSElapsedTime',
					'DepDelay', 'Distance', 'ArrDelay']
	imputer = Imputer(inputCols=numeric_cols, outputCols=numeric_cols).setStrategy("mean")
	data_parsed = imputer.fit(data_parsed).transform(data_parsed)

	# Index categorical columns
	categorical_cols = ['UniqueCarrier', 'TailNum', 'Origin', 'Dest']
	indexer_models = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="skip").fit(data_parsed) for col in categorical_cols]

	# Apply indexers to the DataFrame
	for model in indexer_models:
		data_parsed = model.transform(data_parsed)

	# Define feature columns excluding 'ArrDelay' and original categorical columns
	feature_columns = [
		'DayOfWeek', 'DepTime_timestamp', 'CRSDepTime_timestamp', 'CRSArrTime_timestamp', 'FlightNum', 'CRSElapsedTime',
		'DepDelay', 'Distance', 'UniqueCarrier_index', 'TailNum_index', 'Origin_index', 'Dest_index'
	]

	# Assemble features into a vector
	assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
	vectorized_df = assembler.transform(data_parsed)

	selector = UnivariateFeatureSelector(featuresCol="features", outputCol="selectedFeatures",
										 labelCol="ArrDelay", selectionMode="fpr").setSelectionThreshold(0.05)
	selector.setFeatureType("continuous").setLabelType("continuous").setSelectionThreshold(1)

	result = selector.fit(vectorized_df).transform(vectorized_df)

	# Split the data into training and test sets
	train_data, test_data = result.randomSplit([0.8, 0.2], seed=42)

	# Check for nulls in the label column again after splitting
	if train_data.filter(col("ArrDelay").isNull()).count() > 0 or train_data.filter(isnan(col("ArrDelay"))).count() > 0:
		raise ValueError("The label column still contains null or NaN values after preprocessing.")

	if train_data.count() > 0:
		lr = LinearRegression(featuresCol="selectedFeatures", labelCol="ArrDelay")
		lr_model = lr.fit(train_data)

		# Make predictions
		predictions = lr_model.transform(test_data)
		# ... [The rest of your model prediction and evaluation code] ...
	else:
		raise Exception("No valid data to train on.")

	# Show predictions
	predictions.select("prediction", "ArrDelay", "selectedFeatures").show()

	# Evaluate the model
	evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="rmse")
	rmse = evaluator.evaluate(predictions)
	print("Root Mean Squared Error (RMSE) on test data =", rmse)

	# Stop the SparkSession
	spark_session.stop()
