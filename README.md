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

# Data dictionary
**1. Geographic features**

- **Field_ID:** A unique identifier for each field (BigInt).
 
- **Elevation:** The elevation of the field above sea level in metres (Float).

- **Latitude:** Geographical latitude of the field in degrees (Float).

- **Longitude:** Geographical longitude of the field in degrees (Float).

- **Location:** Province the field is in (Text).

- **Slope:** The slope of the land in the field (Float).

**2. Weather features**

- **Field_ID:** Corresponding field identifier (BigInt).

- **Rainfall:** Amount of rainfall in the area in mm (Float).

- **Min_temperature_C:** Average minimum temperature recorded in Celsius (Float).

- **Max_temperature_C:** Average maximum temperature recorded in Celsius (Float).

- **Ave_temps:** Average temperature in Celcius (Float).

**3. Soil and crop features**

- **Field_ID:** Corresponding field identifier (BigInt).

- **Soil_fertility:** A measure of soil fertility where 0 is infertile soil, and 1 is very fertile soil (Float).

- **Soil_type:** Type of soil present in the field (Text).

- **pH:** pH level of the soil, which is a measure of how acidic/basic the soil is (Float).

**4. Farm management features**

- **Field_ID:** Corresponding field identifier (BigInt).

- **Pollution_level:** Level of pollution in the area where 0 is unpolluted and 1 is very polluted (Float).

- **Plot_size:** Size of the plot in the field (Ha) (Float).

- **Chosen_crop:** Type of crop chosen for cultivation (Text).

- **Annual_yield:** Annual yield from the field (Float). This is the total output of the field. The field size and type of crop will affect the Annual Yield

- **Standard_yield:** Standardised yield expected from the field, normalised per crop (Float). This is independent of field size, or crop type. Multiplying this number by the field size, and average crop yield will give the Annual_Yield.

<br>

**Weather_station_data (CSV)**

- **Weather_station_ID:** The weather station the data originated from. (Int)

- **Message:** The weather data was captured by sensors at the stations, in the format of text messages.(Str)

**Weather_data_field_mapping (CSV)**

- **Field_ID:** The id of the field that is connected to a weather station. This is the key we can use to join the weather station ID to the original data. (Int)

- **Weather_station_ID:** The weather station that is connected to a field. If a field has `weather_station_ID = 0` then that field is closest to weather station 0. (Int)

<br>
