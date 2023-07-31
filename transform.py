#transform.py

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import List
from tqdm import tqdm
import configparser
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, DoubleType, StringType
from pyspark.sql.functions import weekofyear, date_format


# Set up logging
os.makedirs('./input', exist_ok=True)
os.makedirs('./output', exist_ok=True)
logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Configure Spark session
spark = SparkSession.builder \
    .appName('csvUnion') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

def read_csv(file_path: str) -> DataFrame:
    """
    Function to read csv files.
    """
    try:
        df = spark.read.format("csv").option("header", "true").load(file_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read {file_path}. Error: {str(e)}")
        return None

def preprocess(df: DataFrame) -> DataFrame:
    """
    Function to preprocess a dataframe.
    """
    
    # Add any preprocessing steps here.
    # For now it just returns the original DataFrame
    return df

def union_dataframes(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Function to union two dataframes.
    """
    try:
        df = df1.union(df2)
        return df
    except Exception as e:
        logger.error(f"Failed to union dataframes. Error: {str(e)}")
        return df1  # Return the original dataframe if union fails


def read_lookup(file_path: str) -> DataFrame:
    """
    Function to read lookup CSV file.
    """
    try:
        df = spark.read.format("csv").option("header", "true").load(file_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read lookup file {file_path}. Error: {str(e)}")
        return None

def join_with_lookup(df: DataFrame, lookup_df: DataFrame, df_join_column: str, lookup_df_join_column: str, join_type: str) -> DataFrame:
    """
    Function to join a dataframe with a lookup dataframe.
    """
    try:
        df = df.join(lookup_df, df[df_join_column] == lookup_df[lookup_df_join_column], join_type)
        return df
    except Exception as e:
        print("ERROR! join_with_lookup")
        logger.error(f"Failed to join dataframes. Error: {str(e)}")
        return df  # Return the original dataframe if join fails
    
def write_to_csv(df: DataFrame, output_path: str) -> None:
    """
    Function to write dataframe to csv.
    """
    try:
        df.repartition(1).write.csv(output_path, header='true')
        logger.info(f"Data has been successfully written to '{output_path}'")
    except Exception as e:
        logger.error(f"Failed to write data to CSV. Error: {str(e)}")

def dataframe_stats(df: DataFrame) -> None:
    """
    Function to display basic statistics of a dataframe.
    """
    df.printSchema()
    num_rows = df.count()
    num_rows_formatted = format(num_rows, ',')  # Formatting number with thousands separator
    print(f"RESULT: {num_rows_formatted} ROWS\n")
    df.unpersist()

def change_data_types(df: DataFrame) -> DataFrame:
    """
    Function to change data types of the DataFrame.
    """
    date_columns = ["Date"]  # add here the name of the columns that need to be DateType
    double_columns = ["Spend in Local Currency", "Spend (USD)", "Impressions"]  # add here the names of the columns that need to be DoubleType
    string_columns = [col_name for col_name in df.columns if col_name not in date_columns + double_columns]  # the rest will be StringType

    for col_name in date_columns:
        df = df.withColumn(col_name, col(col_name).cast(DateType()))

    for col_name in double_columns:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    for col_name in string_columns:
        df = df.withColumn(col_name, col(col_name).cast(StringType()))

    return df

def add_derived_date_columns(df: DataFrame) -> DataFrame:
    """
    Function to add derived date columns.
    """
    df = df.withColumn("date_year_id", date_format("Date", "yyyy"))
    df = df.withColumn("date_month_id", date_format("Date", "MM"))
    df = df.withColumn("date_week_id", weekofyear(df.Date).cast(StringType()))
    df = df.withColumn("date_id", date_format("Date", "yyyy-MM-dd"))

    return df

def main_pathmatics() -> DataFrame:
    # Directory containing the data
    data_dir = config.get('Paths', 'DataDir')

    # List of CSV files
    csv_files = [
        data_dir + "/PATHMATICS-20190101-20191231.csv",
        data_dir + "/PATHMATICS-20200101-20201231.csv",
        data_dir + "/PATHMATICS-20210101-20211231.csv",
        data_dir + "/PATHMATICS-20220101-20221231.csv",
        data_dir + "/PATHMATICS-20230101-20230723.csv"
    ]

    # Initialize a null DataFrame
    df = None

    # Read, preprocess, and union CSV files
    for file in tqdm(csv_files, desc="Processing CSV files"):
        temp_df = read_csv(file)
        if temp_df is not None:
            df = temp_df if df is None else union_dataframes(df, temp_df)
            
    print(f"\033[92mThe DataFrame has been successfully loaded.\033[0m")
    dataframe_stats(df)

    # Define joins
    joins = [
        {"lookup_table": 'BrandrootLookupTable', "df_join_column": "Brand Root", "lookup_df_join_column": "brandroot_id", "join_type": "inner"},
        {"lookup_table": 'AirlineLookupTable', "df_join_column": "brandroot_airline", "lookup_df_join_column": "airline_id", "join_type": "inner"},
        {"lookup_table": 'PublisherLookupTable', "df_join_column": "Publisher", "lookup_df_join_column": "publisher_id", "join_type": "left_outer"},
        {"lookup_table": 'RegionLookupTable', "df_join_column": "Region", "lookup_df_join_column": "region_id", "join_type": "left_outer"},
        {"lookup_table": 'MediaFormatLookupTable', "df_join_column": "Type", "lookup_df_join_column": "media_format_id", "join_type": "left_outer"}
    ]

    # Perform joins
    for i, join in enumerate(joins, start=1):
        print(f"\nInitiating {i}th join...")
        lookup_df = read_lookup(config.get('Paths', join["lookup_table"]))
        df = join_with_lookup(df, lookup_df, join["df_join_column"], join["lookup_df_join_column"], join["join_type"])
        print(f"\033[92mThe DataFrame has been successfully joined.\033[0m")
        dataframe_stats(df)

    # Change data types
    print(f"Updated Data Types:")
    df = change_data_types(df)
    dataframe_stats(df)

    # Add date derivation columns
    print(f"Added Date Derivations:")
    df = add_derived_date_columns(df)
    dataframe_stats(df)

    if df is not None:
        return df

def main_vivvix() -> DataFrame:
    # Directory containing the data
    data_dir = config.get('Paths', 'DataDir')

    # List of CSV files
    csv_files = [
        data_dir + "/VIVVIX_AIRLINE_2023_DATA.csv"
    ]

    # Initialize a null DataFrame
    df = None

    # Read, preprocess, and union CSV files
    for file in tqdm(csv_files, desc="Processing CSV files"):
        temp_df = read_csv(file)
        if temp_df is not None:
            df = temp_df if df is None else union_dataframes(df, temp_df)
            
    print(f"\033[92mThe DataFrame has been successfully loaded.\033[0m")
    dataframe_stats(df)

    # Define joins
    joins = [
        {
            "lookup_table": 'BrandLookupTable', 
            "df_join_column": "Brand", 
            "lookup_df_join_column": "brand_id", 
            "join_type": "left_outer"
        },
        {
            "lookup_table": 'AirlineLookupTable', 
            "df_join_column": "airline_name", 
            "lookup_df_join_column": "airline_id", 
            "join_type": "left_outer"
        },
        {
            "lookup_table": 'RegionLookupTable', 
            "df_join_column": "Provinces + National Total", 
            "lookup_df_join_column": "region_id", 
            "join_type": "left_outer"
        },
        {
            "lookup_table": 'MediaFormatLookupTable', 
            "df_join_column": "Media Type", 
            "lookup_df_join_column": "media_format_id", 
            "join_type": "left_outer"
        },
    ]

    # Perform joins
    for i, join in enumerate(joins, start=1):
        print(f"\nInitiating {i}th join...")
        lookup_df = read_lookup(config.get('Paths', join["lookup_table"]))
        df = join_with_lookup(df, lookup_df, join["df_join_column"], join["lookup_df_join_column"], join["join_type"])
        print(f"\033[92mThe DataFrame has been successfully joined.\033[0m")
        dataframe_stats(df)

    # Filter duplicated Vivvix national data
    df = df.filter(df.region_id != "National")  # Exclude rows where region_id is "National"
    print(f"\033[92mThe DataFrame has been successfully filtered.\033[0m")
    dataframe_stats(df)

    if df is not None:
        return df

if __name__ == "__main__":

    pathmatics = main_pathmatics()
    write_to_csv(pathmatics, config.get('Paths', 'OutputPathmatics'))
    print("OUTPUT PATHMATICS DATA SUCCESSFULLY.\n")

    vivvix = main_vivvix()
    write_to_csv(vivvix, config.get('Paths', 'OutputVivvix'))
    print("OUTPUT VIVVIX DATA SUCCESSFULLY.\n")

    # Stop the SparkSession
    spark.stop()
