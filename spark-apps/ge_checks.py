import sys
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe  # 1.0 requires importing expectations

def run_validation():
    try:
        
        df = pd.read_parquet("/data/silver/taxi")
        print("----- Data loaded into Pandas.")

        
        context = gx.get_context()

        data_source = context.data_sources.add_pandas("pandas_ds")
        
        
        data_asset = data_source.add_dataframe_asset(name="silver_taxi_asset")
        
        
        batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_taxi_batch_def")

        
        suite_name = "silver_taxi_suite"
        
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

        
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="fare_amount"))

        
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
            column="fare_amount", 
            min_value=0.1, 
            max_value=500.0
        ))

        
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
        )

        
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        
        validation_results = batch.validate(suite)

        
        print(validation_results.describe_dict())

        
        if not validation_results.success:
            print("\n -----DATA VALIDATION FAILED")
            sys.exit(1)

        print("\n----- DATA VALIDATION PASSED")

    except Exception as e:
        print(f"An error occurred: {e}")
       
        print("Note: Ensure you are running Great Expectations v1.0+")
        sys.exit(1)

if __name__ == "__main__":
    run_validation()