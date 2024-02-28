"""
field_data_processor Module

This module defines the FieldDataProcessor class, which encapsulates the data processing logic for field-related data.
The class provides methods to ingest SQL data, rename columns, apply corrections, map weather stations, and execute
the overall data processing pipeline. It is designed to enhance code modularity and readability.

Classes:
    FieldDataProcessor: A class for processing field-related data.

Functions:
    No standalone functions are provided. All functionality is encapsulated within the FieldDataProcessor class.
"""
### START FUNCTION
import logging
import pandas as pd
from data_ingestion import read_from_web_CSV, create_db_engine, query_data


class FieldDataProcessor:
    """
    A class for processing field-related data.

    Parameters:
    - config_params (dict): A dictionary containing configuration parameters.
    - logging_level (str): Logging level for the class (default is "INFO").
    """

    def __init__(self, config_params, logging_level="INFO"):  # Make sure to add this line, passing in config_params to the class 
        """
        Initializes the FieldDataProcessor class.

        Parameters:
        - config_params (dict): A dictionary containing configuration parameters.
        - logging_level (str): Logging level for the class (default is "INFO").
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        # Add the rest of your class code here
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.

        Parameters:
        - logging_level (str): Logging level for the class.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Ingests data from an SQL database.

        Returns:
        - pd.DataFrame: The ingested DataFrame.
        """
        try:
            self.engine = create_db_engine(self.db_path)
            self.df = query_data(self.engine, self.sql_query)
            self.logger.info("Sucessfully loaded data.")
            return self.df

        except Exception as e:
            self.logger.error(f"Failed to ingest SQL data. Error: {e}")
            raise e
    
    def rename_columns(self):
        """
        Swaps the names of specified columns in the DataFrame.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0] 
        
        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})  
        
        self.logger.info(f"Swapped columns: {column1} with {column2}")
    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        Parameters:
        - column_name (str): Name of the column to be corrected (default is 'Crop_type').
        - abs_column (str): Name of the column to take absolute values (default is 'Elevation').
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))
        self.df[column_name] = self.df[column_name].str.strip()


    def weather_station_mapping(self):
        """
        Map weather station data to the main DataFrame based on the 'Field_ID'.

        This function reads weather station data from a web source and performs a left merge with the main DataFrame.
        The merge is based on the common column 'Field_ID'. If successful, the merged data is stored in the main DataFrame.

        Returns:
        None

        Raises:
        Exception: If there is an error during the operation (e.g., failure to read or merge data).
        """
        try:
            weather_map_df = read_from_web_CSV(self.weather_map_data)
            self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
            #self.logger.info("Merged weather station data to the main DataFrame.")
        except Exception as e:
            self.logger.error(f"Failed to map weather stations. Error: {e}")
            raise e

    def process(self):
        """
        Processes data using multiple methods in sequence.
        """
        self.ingest_sql_data()
        #Insert your code here
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()    
      
        
### END FUNCTION
