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
        # Note that by SQL rules, will return Null when value is Null
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
        
        # Appending the boolean column indicating set inclusion (or not)
        # Note that by SQL rules, will return Null when value is Null
        self.df = self.df.withColumn(column + "_in_set", self.df[column].isin(set))
        
        # Returning the modified dataframe
        return self.df
    
    # Constructing method that adds a boolean column indicating
    # whether the values of a pre-existing column are Null or not
    def is_null(self, column: str):
        # Appending a boolean column indicating whether or not each value is Null
        self.df = self.df.withColumn(column + "_is_null", self.df[column].isNull())
        
        # Returning the modified dataframe
        return self.df
    
    # Constructing method to return min and mix of user-provided numeric column
    # across any user-specified grouping variable
    # If no column specified, min and max returned across all numeric columns
    def calc_min_max(self, column: str | None = None, groupby: str | None = None):
        # Confirming column is a numeric column (when specified)
        # Returning None if not
        # Returning min's and max's if so
        if column is not None:
            if dict(self.df.dtypes)[column] not in ["float", "int", "long", "bigint", "double", "integer"]:
                print("Column must be a numeric type")
                return None 
            elif groupby is not None: # Conditional min and max case
                pd_df = self.df.groupBy(groupby).agg(F.min(column), F.max(column)).toPandas()
            else: # Unconditional case
                pd_df = self.df.select(F.min(column), F.max(column)).toPandas()
        # Returning min's and max's for all numeric variables
        # when no columns specified
        else:
            # Capturing numeric variables 
            types = pd.DataFrame(self.df.dtypes, columns = ["variable", "type"])
            numcols = types[types.type.isin(["float", "int", "long", "bigint", "double", "integer"])].variable
            
            # Generating min's and max's in unconditional case
            if groupby is None:
                # Generating min and max and capturing in a dataframe
                # Starting by creating an iteration counter
                iter = 0
            
                # Looping over numeric variables to concatenate their min's and max's in a pandas df
                for col in numcols:
                    if iter == 0:
                        pd_df = self.df.select(F.min(col), F.max(col)).toPandas()
                    else:
                        pd_df = pd.concat([pd_df, self.df.select(F.min(col), F.max(col)).toPandas()], axis = 1)
                    
                    iter += 1
                        
            # Generating min's anbd max's for conditional case
            else:
                # Generating min and max and capturing in a dataframe
                # Starting by creating an iteration counter
                iter = 0

                # Looping over numeric variables to concatenate their min's and max's in a pandas df                
                for col in numcols:
                    if iter == 0:
                        pd_df = self.df.groupBy(groupby).agg(F.min(col), F.max(col)).toPandas()
                    else:
                        pd_df = pd.concat([pd_df, self.df.groupBy(groupby).agg(F.min(col), F.max(col)) \
                                           .toPandas().drop(columns = groupby)], axis = 1)
                    iter += 1
                        
                        
        # Returning the resultant pandas dataframe of min's and max's
        return pd_df
    
    # Constructing method to return counts for levels of a single categorical variable or 
    # combinations of levels for two categorical variables
    def count_combos(self, column1: str, column2: str | None = None):
        # Confirming at least one of the two input columns is actually a string
        if dict(self.df.dtypes)[column1] != "string":
            # If first column not, check second column (if not None)
            if column2 is None or dict(self.df.dtypes)[column2] != "string":
                # If no string columns provided, print message and return None
                print("Each input column is numeric, but should be string")
                return None 
            
            # Case when second column is a string
            else: 
                # Printing message indicating first column numeric
                print("column1 is numeric, should be string")
                
                # Generating counts for each level of column2
                pd_df = self.df.groupBy(column2).count().toPandas()
        
        # Case when first column is a string
        else:
            # Case when second column not provided
            if column2 is None:
                # Generating counts for each level of column1
                pd_df = self.df.groupBy(column1).count().toPandas()
            
            # Case when second column is numeric
            elif dict(self.df.dtypes)[column2] != "string":
                # Printing message indicating second column numeric
                print("column2 is numeric, should be string")
                
                # Generating counts for each level of column1
                pd_df = self.df.groupBy(column1).count().toPandas()
                
            # Case when both columns are strings
            else:
                # Generating counts for each combination of levels
                pd_df = self.df.groupBy([column1, column2]).count().toPandas()
                
        # Returning the pandas data frame of counts
        return pd_df
    
    
        
            
            
        
    
    
        
        
        
        
    
    
        
        
    