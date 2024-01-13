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

	# load data
	data_reader = DataReaderManager.get_data_reader(spark_session=spark_session)
	data = data_reader.read_all_files()

	# initial data processing
	data_processor = DataProcessor(input_data=data)
	data_parsed = data_processor.get_processed()

	# fill nulls with mean for empty numeric values
	numeric_cols = ['DepTime_timestamp', 'CRSDepTime_timestamp', 'CRSArrTime_timestamp', 'FlightNum', 'CRSElapsedTime',
					'DepDelay', 'Distance', 'ArrDelay', "TaxiOut"]
	imputer = Imputer(inputCols=numeric_cols, outputCols=numeric_cols).setStrategy("mean")
	data_parsed = imputer.fit(data_parsed).transform(data_parsed)

	# convert categorical attributes
	categorical_cols = ['UniqueCarrier', 'TailNum', 'Origin', 'Dest']
	indexer_models = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="skip").fit(data_parsed)
					  for col in categorical_cols]
	for model in indexer_models:
		data_parsed = model.transform(data_parsed)

	# Define feature columns to be used as independent variables. Dependent variable is ArrDelay
	feature_columns = [
		'DayOfWeek', 'DepTime_timestamp', 'CRSDepTime_timestamp', 'CRSArrTime_timestamp', 'FlightNum', 'CRSElapsedTime',
		'DepDelay', 'Distance', 'UniqueCarrier_index', 'TailNum_index', 'Origin_index', 'Dest_index', 'TaxiOut'
	]

	# Assemble features into a vector
	assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
	vectorized_df = assembler.transform(data_parsed)

	# Select features
	selector = UnivariateFeatureSelector(featuresCol="features", outputCol="selectedFeatures",
										 labelCol="ArrDelay", selectionMode="fpr")
	selector.setFeatureType("continuous").setLabelType("continuous").setSelectionThreshold(0.05)
	selector_model = selector.fit(vectorized_df)
	selected_df = selector_model.transform(vectorized_df)

	selected_feature_names = [feature_columns[i] for i in selector_model.selectedFeatures]
	print("Selected feature names:", selected_feature_names)

	# Split the data into training and test sets
	train_data, test_data = selected_df.randomSplit([0.8, 0.2], seed=42)

	# Check for nulls in the label column again after splitting
	if train_data.filter(col("ArrDelay").isNull()).count() > 0 or train_data.filter(isnan(col("ArrDelay"))).count() > 0:
		raise ValueError("The label column still contains null or NaN values after preprocessing.")

	# Train a model
	if train_data.count() > 0:
		lr = LinearRegression(featuresCol="selectedFeatures", labelCol="ArrDelay")
		lr_model = lr.fit(train_data)

		predictions = lr_model.transform(test_data)
	else:
		raise Exception("No valid data to train on.")

	# Evaluate the model
	rmse_evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="rmse")
	r2_evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="r2")

	rmse = rmse_evaluator.evaluate(predictions)
	r2 = r2_evaluator.evaluate(predictions)

	print()
	print("Root Mean Squared Error (RMSE) on test data =", rmse)
	print("R-squared on test data =", r2)

	# Stop the SparkSession
	spark_session.stop()
