# PySpark dependencies:s
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import pyspark.sql.types as T
from pyspark.sql.window import Window

# Sedona dependencies:
from sedona.utils.adapter import Adapter
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from sedona.core.SpatialRDD import SpatialRDD
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.core.formatMapper import GeoJsonReader

# database utilities:
from sqlalchemy import create_engine
import sqlite3 as db
import pandas as pd
import geopandas as gpd

# other relevant libraries:
import inflection
import unicodedata
import re
import os
from glob import glob
import shutil
import itertools
from collections import Counter
import chardet


# defining a function for better handling of pyspark parquet file outputs:
def save_to_filesystem(df, target_path, parquet_path, filename):
    """Helper function to save pyspark dataframes as parquets in a way that is similar to writing to local files

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe to be saved
        target_path (str): path that will store the file
        filename (str): name of the resulting file

    Returns:
        None
    """
    PARQUET_FILE = f"{target_path}/{parquet_path}"
    OUTPUT_FILE = f"{target_path}/{filename}"

    if os.path.exists(PARQUET_FILE):
        shutil.rmtree(
            PARQUET_FILE
        )  # if the directory already exists, remove it (throws error if not)

    # saves the dataframe:
    df.coalesce(1).write.save(PARQUET_FILE)

    # retrieves file resulting from the saving procedure:
    original_file = glob(f"{PARQUET_FILE}/*.parquet")[0]

    # renames the resulting file and saves it to the target directory:
    os.rename(original_file, OUTPUT_FILE)

    shutil.rmtree(PARQUET_FILE)

    return True


def save_as_table(df, table_name, database):
    """Helper function to save a pyspark dataframe to sqlite database as a table

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe to be written
        table_name (str): name of the table in sqlite
        database (str): path to the target database

    Returns:
        None

    """

    # instantiating the sqlite database:
    conn = db.connect("../data/database/db_ecommerce.db")

    # converting the pyspark dataframe to pandas:
    pdf = df.toPandas()

    # saving the dataset:
    print(f"Saving dataframe to table {table_name}")
    pdf.to_sql(table_name, conn, if_exists="replace", index=False)

    return True


def rotate_xticks(ax, rotation):
    """Helper function to rotate x-axis labels of a matplotlib-based plot

    Args:
        ax (axes.AxesSubplot): matplotlib subplot axis object that handles the figure
        rotation (int): degrees for which to rotate the axis x-ticks

    Returns:
        None

    """
    for item in ax.get_xticklabels():
        item.set_rotation(rotation)

def get_file_encoding(filename, is_partial=False):
    if is_partial == True:
        with open(filename, "rb") as input_file:
            start, end = (0, 20000)
            input_file.seek(start)
            print(chardet.detect(input_file.read(end - start)))
    else:
        with open(filename, "rb") as input_file:
            print(chardet.detect(input_file.read()))

def sort_dataframe_columns(df, leading_col=None):

    col_order = sorted(df.columns)

    if leading_col is not None:
        col_order.remove(leading_col)
        col_order.insert(0, leading_col)

    df = df.select(*col_order)

    return df

def load_geospatial_file(file_path, spark_session):
    shape_rdd = ShapefileReader.readToGeometryRDD(sc=spark_session, inputPath=file_path)

    df = Adapter.toDf(shape_rdd, spark_session)

    return df

def save_geospatial_file(df, file_path):

    df = gpd.GeoDataFrame(df.toPandas())

    df.to_parquet(file_path)

    return True

def convert_geometry(df, geometry_col, input_crs, output_crs):
    df = df.withColumn(
        geometry_col,
        F.expr(
            f"ST_FlipCoordinates(ST_Transform({geometry_col}, '{input_crs}', '{output_crs}'))"
        ),
    )

    return df

def rank_feature(df, col, ascending=True):
    """Helper function to generate rank-based features given a column in PySpark dataframe

    Args:
        df (pyspark.sql.dataframe.DataFrame):
        col (str): the name of the column to be used for calculating the window
        ascending (bool): a boolean indicator as to what type of ranking to be made (ascending or descending)

    """
    # specifying a window function for the operation:
    if ascending:
        window_spec = Window.orderBy(col)
    else:
        window_spec = Window.orderBy(F.desc(col))

    # adding the window function to generate a new column:
    df = df.withColumn(f"score_{col}", F.percent_rank().over(window_spec))

    return df

def replace_decimal_separator(val):
    if val is not None:
        try:
            return val.replace(",", ".")
        except:
            return val
    else:
        return None 

def bulk_aggregate(df, group_col, aggs, target_cols):
    """Wrapper function to apply multiple aggregations when performing group bys

    It utilizes the spark's SQL Context and string interpolation to perform the aggregation using SQL syntax.

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe with raw data
        group_col (str): the column that will be used for grouping
        aggs (list): list of aggregations that want to be made (must be the same name as pyspark.sql.functions)
        target_cols (str): columns in which aggregations will be performed

    Returns:
        df_grouped (pyspark.sql.dataframe.DataFrame): dataframe with the grouped data
    """

    # buils the cartersian product of the lists
    aggs_to_perform = itertools.product(aggs, target_cols)

    Q_LAYOUT = """
    SELECT
        {},
        {}
        FROM df
        GROUP BY {}
    """

    aggregations = []
    for agg, col in aggs_to_perform:

        # builds the string for aggregation
        statement = f"{agg.upper()}({col}) as {agg}_{col}"
        aggregations.append(statement)

    full_statement = ",\n".join(aggregations)

    # uses string interpolation to build the full query statement
    QUERY = Q_LAYOUT.format(group_col, full_statement, group_col)

    # registers the dataframe as temporary table:
    df.registerTempTable("df")
    df_grouped = spark.sql(QUERY)

    # rounds values:
    for column in df_grouped.columns:
        df_grouped = df_grouped.withColumn(column, F.round(F.col(column), 1))

    return df_grouped


def get_null_columns(df, normalize=False):
    """Helper function to print the number of null records for each column of a PySpark DataFrame.

    Args:
        df (pyspark.sql.dataframe.DataFrame): a PySpark Dataframe object

    Returns:
        None -> prints to standard out

    """

    if normalize:
        total = df.count()

        df_nulls = df.select(
            [
                (F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)) / total).alias(column)
                for column in df.columns
            ]
        )

    else:
        df_nulls = df.select(
            [
                F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                for column in df.columns
            ]
        )

    # displaying the results to standard out
    df_nulls.show(1, truncate=False, vertical=True)


######### Text Processing Functions ########
def normalize_entities(text):
    """Helper function to normalize text data to ASCII and lower case, removing spaces

    Args:
        text (string): the string that needs to be normalized

    Returns:
        text (string): cleaned up string

    """
    regex = r"[^a-zA-Z0-9,]+"

    if text is not None:

        text = (
            unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("utf-8")
        )

        text = inflection.underscore(text)
        text = str(text)
        text = text.lower()
        text = re.sub(regex, " ", text)
        text = text.replace(",", ".")
        text = text.replace(".", " ")
        text = text.replace(" ", "_")
        text = text.strip().strip('_')

    return text

def normalize_portuguese_variable_names(text):
    """Helper function to normalize specifically codebook variables from the IBGE census and related datasets

    Args:
        text (string): the string that needs to be normalized

    Returns:
        text (string): cleaned up string

    """   
    re_text = r"[^a-zA-Z0-9]+"

    # removes items inside parenthesis (usually not relevant for column names)
    re_parenthesis = r"\((.*?)\)"

    stopwords = {
        'do', 'da', 'de', 'ou', 'das', 'dos', 'de', 'e', 'a', 'o', 'em', 'no', 'na'
    }

    if text is not None:

        text = (
            unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("utf-8")
        )

        text = str(text)
        text = text.lower()
        # text = re.sub(re_parenthesis, '', text)
        text = text.replace('(', '').replace(')', '')
        text = re.sub(re_text, ' ', text)

        # removing stopwords:
        text = '_'.join(list(filter(lambda val: val not in stopwords, text.split(' '))))
        
        # removing consecutive sequential words:
        splits = text.split('_')
        uniques = [item[0] for item in itertools.groupby(splits)]
        clean_text = '_'.join(uniques).strip('_')

        return clean_text 
    
    else:
        return 'N/A'

def standardize_variable_names(text):
    """Helper function to standardize variable names from the Census Data.

    Args:
        text (string): the string that needs to be normalized

    Returns:
        text (string): cleaned up string

    """
    regex = r"[^a-zA-Z0-9,]+"

    stopwords = {
        'in',
        'on',
        'of',
        'the'
    }

    if text is not None:
        text = str(text)
        text = text.lower()
        text = re.sub(regex, " ", text)
        text = text.replace(",", "")

        # removing consecutive sequential words:
        splits = text.split(' ')

        # removing stopwords:
        splits = list(filter(lambda val: val not in stopwords, splits))
        text = '_'.join(splits).strip('_').strip(' ')

    return text

def drop_invalid_census_columns(df, codebook, dataset_name):

    # dropping unspecified columns (problems with the raw files):
    df = df.drop(*[col for col in df.columns if "_c" in col])

    # retrieving the columns:
    original_columns = sorted(df.columns)

    # normalize column names:
    normalized_columns = list(map(clean_census_column_name, original_columns))
    
    # dropping columns that do not exist in the codebook:
    cols_to_drop = []
    cols_filter = []
    for i in range(len(normalized_columns)):

        col = normalized_columns[i]

        if col not in list(codebook[dataset_name].keys()):

            # keep track of the columns that need to be dropped
            cols_to_drop.append(original_columns[i])
            cols_filter.append(normalized_columns[i])
            
    print(f'Dropping columns {cols_to_drop} - not present in the codebook')
    df = df.drop(*cols_to_drop)  

    normalized_columns = [col for col in normalized_columns if col not in cols_filter]
    original_columns = [col for col in original_columns if col not in cols_to_drop]

    # looking up column names:
    new_columns = list(
        map(lambda col: codebook[dataset_name][col], normalized_columns)
    )

    # get columns with duplicates:
    cols_map = dict(zip(original_columns, new_columns))

    reverse_dictionary = {}
    for key, value in cols_map.items():
        reverse_dictionary.setdefault(value, set()).add(key)

    duplicates = list(
        set(
            itertools.chain.from_iterable(
                values for key, values in reverse_dictionary.items() if len(values) > 1
            )
        )
    )

    # dropping the duplicates:
    df = df.drop(*duplicates)

    print(f"Dropping columns {duplicates} for being duplicates")

    return df

def get_column_values(df, col):
    # collect the records:
    rows = list(df.select(col).distinct().collect())
    return [row[col] for row in rows]

def get_file_crs(file_path):
    # read the file into a geopandas dataframe:
    df = gpd.read_file(
        file_path,
        nrows = 10
    )

    return df.crs

def clean_census_column_name(text):
    text = (
        unicodedata.normalize("NFD", text)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .strip()
        .lower()
        .replace(" ", "_")
    )

    if (text == "situacao"):
        text = "situacao_setor"

    return text

def convert_to_geopandas(df, geometry_column):
    """Helper function to convert spark dataframes with Sedona objects to Geopandas"""
    # extracting columns
    columns = df.columns

    gdf = gpd.GeoDataFrame(
        df.rdd.map(lambda row: [row[col] for col in columns]).collect(),
        columns=columns,
        geometry=geometry_column,
    )

    return gdf

def apply_category_map(category_map):
    """Helper function to convert strings given a map

    Note:
        This function uses the function generator scheme, much like the PySpark code

    Args:
        original_category (str): the original category name
        category_map (dict): the hash table or dictionary for converting the values:

    Returns:
        new_category (str): the resulting category

    """

    def func(row):
        try:
            result = category_map[row]
        except:
            result = None
        return result

    return F.udf(func)


def normalize_column_name(col):
    """Helper function standardize column names

    Args:
        col (str): the column name as a string

    Returns:
        new_col (str): the normalized version of the column name
    """
    # lower case the records
    col = col.lower()

    # replaces spaces for underscores
    col = col.replace(" ", "_")

    # normalizes text to ascii:
    new_col = (
        unicodedata.normalize("NFD", col).encode("ascii", "ignore").decode("utf-8")
    )
    return new_col


######### Feature extraction #######
def get_datetime_features(df, time_col):
    """Function to extract time-based features from pyspark dataframes

    Args:
        df (pyspark.sql.dataframe.DataFrame): the original dataframe that needs to be enriched
        time_col (str): the string name of the column containing the date object

    Returns:
        df (pyspark.sql.dataframe.DataFrame): resulting pyspark dataframe with the added features
            -> See list of attribute the source code for the attributes

    """

    # applying date-related functions:

    # day-level attributes:
    df = df.withColumn("day_of_week", F.dayofweek(F.col(time_col)))

    df = df.withColumn("day_of_month", F.dayofmonth(F.col(time_col)))

    df = df.withColumn("day_of_year", F.dayofyear(F.col(time_col)))

    # week-level attributes:
    df = df.withColumn("week_of_year", F.weekofyear(F.col(time_col)))

    # month-level attributes:
    df = df.withColumn("month", F.month(F.col(time_col)))

    df = df.withColumn("quarter", F.quarter(F.col(time_col)))

    # year-level attributes:
    df = df.withColumn("year", F.year(F.col(time_col)))

    return df
