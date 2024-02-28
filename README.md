# Integrated project: Validating our data

In this project I am diving into the agricultural dataset again to continue to validate our data. First I build a data pipeline that will ingest and clean our data with the press of a button, cleaning up our code significantly. 

The code is organised into three modules: 

    a. `data_ingesation.py` - All SQL-related functions, and web-based data retrieval.

    b. `field_data_processor.py` - All transformations, cleanup, and merging functionality.

    c. `weather_data_processor.py` - All transformations and cleanup of the weather station data.

There is an additional `validate_data.py` file . This is a `pytest` script that does a couple of tests to see if the data we're expecting, is what we actually have. 
