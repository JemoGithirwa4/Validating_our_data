# Integrated project: Validating our data

In this project I am diving into the agricultural dataset again to continue to validate our data. First I build a data pipeline that will ingest and clean our data with the press of a button, cleaning up our code significantly. 

The code is organised into three modules: 

    a. `data_ingesation.py` - All SQL-related functions, and web-based data retrieval.

    b. `field_data_processor.py` - All transformations, cleanup, and merging functionality.

    c. `weather_data_processor.py` - All transformations and cleanup of the weather station data.

There is an additional `validate_data.py` file . This is a `pytest` script that does a couple of tests to see if the data we're expecting, is what we actually have. 

The main goal of this project is: Is the data in our `MD_agric_df` dataset representative of reality? To answer this, I use weather-related data from nearby stations to validate the results. If the weather data matches the data I have, we can be more confident that our dataset represents reality. 

So what's the plan? 
1. Create a null hypothesis.
1. Import the `MD_agric_df` dataset and clean it up.
1. Import the weather data.
1. Map the weather data to the field data.
1. Calculate the means of the weather station dataset and the means of the main dataset.
2. Calculate all the parameters we need to do a t-test. 
3. Interpret our results.
