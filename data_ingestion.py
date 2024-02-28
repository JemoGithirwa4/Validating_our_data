"""data_ingestion Module

This module contains functions for data ingestion, including creating a database engine,
querying data from a database, and reading CSV data from the web.

Attributes:
    logger (Logger): A logger object for recording messages related to data ingestion.

"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

def create_db_engine(db_path):
    """Create Database Engine

    Creates a SQLAlchemy database engine.

    Args:
        db_path (str): The path to the SQLite database.

    Returns:
        sqlalchemy.engine.Engine: The database engine.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: If there is an error creating the database engine.

    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """Query Data from Database

    Executes a SQL query on the provided database engine and returns the result as a DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        sql_query (str): The SQL query string.

    Returns:
        pandas.DataFrame: The result of the SQL query.

    Raises:
        ValueError: If the query returns an empty DataFrame.
        Exception: If there is an error executing the SQL query.

    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """Read CSV Data from the Web

    Reads CSV data from the provided URL and returns it as a DataFrame.

    Args:
        URL (str): The URL pointing to the CSV file.

    Returns:
        pandas.DataFrame: The CSV data as a DataFrame.

    Raises:
        pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
        Exception: If there is an error reading the CSV from the web.

    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION
