# Loading in necessary modules and functions
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


class SparkDataCheck: 
    '''
    To be written
    '''

    # Initializing the class
    def __init__(self, df: DataFrame):
        # Creating a df attribute containing the dataframe input
        self.df = df
        
        
    # Constructing method for creating a new instance while reading in a csv
    @classmethod
    def from_csv(cls, spark, csv_path: str): 
        # Reading in the file as a SQL dataframe
        # Note: We are assuming the file has headers
        df = spark.read.load(csv_path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        
        # Returning the dataset as an object of the SparkDataCheck class
        return cls(df)
    
    # Constructing method for creating a new instance from a pandas dataframe
    @classmethod
    def from_pddf(cls, spark, pd_df):
        # Converting pandas dataframe to SQL-style dataframe
        df = spark.createDataFrame(pd_df)
        
        # Returning the dataset as an object of the SparkDataCheck class
        return cls(df)
    
    # Constructing method that adds a boolean column indicating
    # whether the values of a pre-existing numeric column are within a given range
    def in_range(self, column: str, lower: float | None = None, upper: float | None = None): 
        # Confirming column is a numeric column
        if dict(self.df.dtypes)[column] not in ["float", "int", "long", "bigint", "double", "integer"]:
            print("Column must be a numeric type")
            return self.df
        
        # Checking if neither upper or lower is provided
        if lower is None and upper is None:
            raise ValueError("At least one of lower and upper must be provided.")
        # If at least one provided, constructing new column name and column
        elif lower is None: # case when lower not specified
            new_var = column + "_below_" + str(upper)
            self.df = self.df.withColumn(new_var, self.df[column] <= upper)
        elif upper is None: # case when upper not specified
            new_var = column + "_above_" + str(lower)
            self.df = self.df.withColumn(new_var, self.df[column] >= lower)
        else: # case when both specified
            new_var = column + "_in_" + str(lower) + "_" + str(upper)
            self.df = self.df.withColumn(new_var, self.df[column].between(lower, upper))
            
        # Returning modified dataframe    
        return self.df
    
    # Constructing method that adds a boolean column indicating
    # whether the values of a pre-existing string column are in a given set
    def in_set(self, column: str, set: list[str]):
        # Confirming column is a string column
        if dict(self.df.dtypes)[column] != "string":
            print("Column must be a string type")
            return self.df
        
        # Appending the boolean indicating set inclusion (or not)
        self.df = self.df.withColumn(column + "_in_set", self.df[column].isin(set))
        
        # Returning the modified dataframe
        return self.df
    
    
    
        
        
        
        
    
    
        
        
    
