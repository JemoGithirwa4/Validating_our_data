"""
weather_data_processor.py module

Module for processing weather data, including loading, extracting measurements, and calculating means.

Classes:
- WeatherDataProcessor: A class for processing weather data.

"""
# These are the imports we're going to use in the weather data processing module
import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV

### START FUNCTION 

class WeatherDataProcessor:
    """
    A class for processing weather data, including loading, extracting measurements, and calculating means.

    Attributes:
    - weather_station_data (str): Path to the weather station data CSV file.
    - patterns (dict): Dictionary containing regular expression patterns for measurement extraction.
    - weather_df (pd.DataFrame): DataFrame to store weather data.
    - logger (logging.Logger): Logger for recording information and debugging.

    Methods:
    - __init__(self, config_params, logging_level="INFO"): Constructor for WeatherDataProcessor.
    - initialize_logging(self, logging_level): Initializes the logging configuration.
    - weather_station_mapping(self): Loads weather station data from the web.
    - extract_measurement(self, message): Extracts measurement from a given message using regex patterns.
    - process_messages(self): Processes messages to extract measurements and updates the DataFrame.
    - calculate_means(self): Calculates mean values of measurements grouped by station and type.
    - process(self): Executes the complete data processing pipeline.
    """
    def __init__(self, config_params, logging_level="INFO"):
        """
        Constructor for WeatherDataProcessor.

        Parameters:
        - config_params (dict): Configuration parameters including weather CSV path and regex patterns.
        - logging_level (str): Logging level ("DEBUG", "INFO", "NONE"). Default is "INFO".
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Initializes the logging configuration.

        Parameters:
        - logging_level (str): Logging level ("DEBUG", "INFO", "NONE").
        """
        logger_name = __name__ + ".WeatherDataProcessor"
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

    def weather_station_mapping(self):
        """
        Loads weather station data from the web and initializes weather_df.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    
    def extract_measurement(self, message):
        """
        Extracts measurement from a given message using regex patterns.

        Parameters:
        - message (str): Input message containing measurement information.

        Returns:
        - Tuple (str, float): Measurement type and extracted numeric value. (None, None) if no match.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Processes messages to extract measurements and updates the DataFrame.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculates mean values of measurements grouped by station and type.

        Returns:
        - pd.DataFrame: DataFrame containing mean values for each measurement type and station.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Calculates mean values of measurements grouped by station and type.

        Returns:
        - pd.DataFrame: DataFrame containing mean values for each measurement type and station.
        """
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")
### END FUNCTION
