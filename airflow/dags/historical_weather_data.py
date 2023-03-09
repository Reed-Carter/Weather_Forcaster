import pandas as pd
import requests
import re
import dateutil
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from typing import List
from google.cloud import bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os
# from sklearn.linear_model import Ridge

@task
def read_historical_weather_data():

    #read csv to dataframe
    weather_df = pd.read_csv('./dags/noaa_weather_data.csv', index_col='DATE')

    #rename and filter columns which can be useful
    core_weather_df = weather_df[['NAME','PRCP', 'SNOW','SNWD', 'TMAX','TMIN']].copy()
    core_weather_df.columns = ['name','precip', 'snow', 'snow_depth', 'temp_max','temp_min']

    #fill missing data points
    core_weather_df['precip'] = core_weather_df['precip'].fillna(0)
    core_weather_df['snow'] = core_weather_df['snow'].fillna(0)
    core_weather_df['snow_depth'] = core_weather_df['snow_depth'].fillna(0)
    #map the name to a number variable to be used as a predictor
    core_weather_df['name_num'] = pd.factorize(core_weather_df['name'])[0]
    #'ffill' ('forward fill') fills the value in with the value from the previous date this will apply to the temp_min/max columns since the others have already been filled in
    core_weather_df = core_weather_df.fillna(method='ffill')

    #filter cities with only complete data
    core_weather_df = core_weather_df[core_weather_df.name.isin(['BEMIDJI, MN US', 'SHARJAH INTER. AIRP, AE']) == False]

    #create a column 'target_temp_max' by shifting all values in the temp_max column back a day...creates a column based on temperatures from tomorrows temps
    core_weather_df['target_temp_max_day_1'] = core_weather_df.groupby('name')['temp_max'].shift(-1)
    core_weather_df['target_temp_max_day_2'] = core_weather_df.groupby('name')['target_temp_max_day_1'].shift(-1)
    core_weather_df['target_temp_max_day_3'] = core_weather_df.groupby('name')['target_temp_max_day_2'].shift(-1)
    core_weather_df['target_temp_max_day_4'] = core_weather_df.groupby('name')['target_temp_max_day_3'].shift(-1)
    core_weather_df['target_temp_max_day_5'] = core_weather_df.groupby('name')['target_temp_max_day_4'].shift(-1)
    core_weather_df['target_temp_max_day_6'] = core_weather_df.groupby('name')['target_temp_max_day_5'].shift(-1)
    core_weather_df['target_temp_max_day_7'] = core_weather_df.groupby('name')['target_temp_max_day_6'].shift(-1)
    core_weather_df['target_temp_max_day_8'] = core_weather_df.groupby('name')['target_temp_max_day_7'].shift(-1)
    core_weather_df['target_temp_max_day_9'] = core_weather_df.groupby('name')['target_temp_max_day_8'].shift(-1)
    core_weather_df['target_temp_max_day_10'] = core_weather_df.groupby('name')['target_temp_max_day_9'].shift(-1)
    #remove the last few rows since they are NaN value because that value would be in the future
    core_weather_df = core_weather_df.dropna()

    #create a date column from the index and make it a string type to be passed by the xcoms
    core_weather_df['date'] = core_weather_df.index
    core_weather_df['date']=core_weather_df['date'].astype(str)
    core_weather_dict = core_weather_df.to_dict()

    return core_weather_dict

@task
def load_historical_data_to_bigquery(data):

    PROJECT_ID = "deb-01-372116"
    DATASET_ID = "Weather_Forecaster"
    DAILY_TABLE_ID = "historical_weather_data"

    SCHEMA = [
                # indexes are written if only named in the schema
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('precip', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('snow', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('snow_depth', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('temp_max', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('temp_min', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('name_num', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_1', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_2', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_3', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_4', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_5', 'FLOAT64', mode='REQUIRED'),
                bigquery.SchemaField('target_temp_max_day_6', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_7', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_8', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_9', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('target_temp_max_day_10', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('date', 'DATETIME', mode='NULLABLE'),
            ]

    #change the date column back to a datetime dtype
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])

    client = bigquery.Client()

 
    try:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = client.get_dataset(dataset_ref)
    except:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)

    table_ref = dataset.table(DAILY_TABLE_ID)

    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

@task
def scrape_precip_data():
    
    urls = [
            'https://www.localconditions.com/weather-philadelphia-pennsylvania/19019/past.php',
            'https://www.localconditions.com/weather-orlando-florida/32801/past.php',
            #'https://www.localconditions.com/weather-milwaukee-wisconsin/53201/past.php',
            # 'https://www.localconditions.com/weather-lynchburg-virginia/24501/past.php',
            # 'https://www.localconditions.com/weather-kalispell-montana/59901/past.php',
            # 'https://www.localconditions.com/weather-kahului-hawaii/96732/past.php',
            # 'https://www.localconditions.com/weather-fairbanks-alaska/99701/past.php',
            # 'https://www.localconditions.com/weather-bangor-maine/04401/past.php',
            # 'https://www.localconditions.com/weather-albuquerque-new-mexico/87101/past.php',
            # 'https://www.localconditions.com/weather-portland-oregon/97201/past.php',
            # "https://www.localconditions.com/weather-sitka-alaska/99835/past.php"
            ]

    precip_df = pd.DataFrame()

    for url in urls:
            r = requests.get(url)
            soup = BeautifulSoup(r.content,"html.parser")
            details = soup.select_one(".past_weather_express")
            # Find all div elements with class="panel"
            panel_divs = soup.find_all('div', {'class': 'panel'})
            # Extract the text content of each div element and store it in a list
            panel_texts = [panel_div.text.strip() for panel_div in panel_divs]
            # Print the list of extracted text content
            data = panel_texts[1]
            data = [item.strip() for item in data]
            data = [item for item in data if item]
            data = data[77:90]
            data="".join(data)
            df = pd.DataFrame([data], columns=['precip'])
            precip_df = pd.concat([precip_df, df], ignore_index=True, sort=False)
            
    precip_df['precip'] = precip_df['precip'].str.extract(pat='(\d+\.?\d*)').astype(float)
    precip_df = precip_df.fillna(0)
    precip_dict = precip_df.to_dict()
    
    return precip_dict

@task
def scrape_nws_data():

    urls = [
    #         "https://forecast.weather.gov/MapClick.php?lat=57.0826&lon=-135.2692#.Y-vs_9LMJkg",
    #         'https://forecast.weather.gov/MapClick.php?lat=45.5118&lon=-122.6756#.Y-vtHNLMJkg',
    #         'https://forecast.weather.gov/MapClick.php?lat=35.0842&lon=-106.649#.ZAKIR9LMJhE',
    #         'https://forecast.weather.gov/MapClick.php?lat=44.8017&lon=-68.7708#.ZAKIY9LMJhE',
    #         'https://forecast.weather.gov/MapClick.php?lat=64.8453&lon=-147.7221#.ZAKId9LMJhE',
    #         'https://forecast.weather.gov/MapClick.php?lat=20.8986&lon=-156.4305#.ZAKIjtLMJhE',
    #         'https://forecast.weather.gov/MapClick.php?lat=48.1786&lon=-114.3037#.ZAKIqNLMJhE',
    #         'https://forecast.weather.gov/MapClick.php?lat=37.4142&lon=-79.143#.ZAKIwtLMJhE',
    #        'https://forecast.weather.gov/MapClick.php?lat=42.9467&lon=-87.8967#.ZAKI0tLMJhE',
            'https://forecast.weather.gov/MapClick.php?lat=28.4272&lon=-81.308#.ZAKI5NLMJhE',
            'https://forecast.weather.gov/MapClick.php?lat=39.8784&lon=-75.2402#.ZAKJAdLMJhE'
            ]
            
    combined_df = pd.DataFrame()

    for url in urls:
        r = requests.get(url)
        soup = BeautifulSoup(r.content,"html.parser")

        #various containers
        item1 = soup.find_all(id='current_conditions-summary')
        item2 = soup.find_all(id='current_conditions_detail')
        item4 = soup.find_all(id='tombstone-container')

        #raw data
        temp_f = [item.find(class_="myforecast-current-lrg").get_text() for item in item1]
        temp_min = soup.find('p', {'class': 'temp temp-low'}).text.strip()
        temp_max = soup.find('p', {'class': 'temp temp-high'}).text.strip()


        #df of temperatures
        df_temperature = pd.DataFrame({"temp" : temp_f,'tempmin': temp_min,'tempmax': temp_max})

        #df_2 is a df of current conditions in detail (Humidity, Wind Speed, Barometer, Dewpoint, Visibility, Last update)
        table = soup.find_all('table')
        df_2 = pd.read_html(str(table))[0]
        df_2 = df_2.rename(columns={'1':'metrics'})
        # df_2['1'] = df_2['1'].fillna(0)
        df_2 = df_2.pivot(columns=0, values=1).ffill().dropna().reset_index().drop(columns=['index'])

        #merge both dataframes
        temp_df=pd.concat([df_temperature,df_2],axis=1)

        #scrape lattitude, longitude, and elevation 
        lat_lon_elev = soup.find('span', {'class': 'smallTxt'}).text.strip()
        lat, lon, elev = re.findall(r'[-+]?\d*\.\d+|\d+', lat_lon_elev)

        #scrape name
        station = soup.find('h2', {'class': 'panel-title'}).text.strip()

        #add location, lat, long, and elev to source_df
        temp_df['elevation_ft'] = elev
        temp_df['latitude'] = lat
        temp_df['longitude'] = lon
        temp_df['weather_station'] = station

        combined_df = pd.concat([temp_df, combined_df], ignore_index=True, sort=False)
    
    combined_df = combined_df.fillna(0)
    combined_dict = combined_df.to_dict()

    return combined_dict

@task
def combine_scraped_dataframes(dict1, dict2):
        #create dataframes from the dicts since dicts can not be passed as xcoms
    df1 = pd.DataFrame.from_dict(dict1)
    df2 = pd.DataFrame.from_dict(dict2)

    #combine the two dataframes 
    source_df = pd.concat([df1,df2],axis=1)

    #converty the df back to a dictionary to be passed to the next task and return the dict
    source_dict = source_df.to_dict()
    return source_dict

@task
def transform_scraped_data(dict1):
    #convert the dict to a df to be transformed
    df = pd.DataFrame.from_dict(dict1)
    # Convert 'lat' and 'lon' columns to float type
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)

    # Convert 'elev' column to int type
    df['elevation_ft'] = df['elevation_ft'].astype(int)

    # Extract the numeric part of the temperature string and convert it to int
    df['temp'] = df['temp'].str.extract('(\d+)').astype(float)

    # Extract the numeric part of the tempmin string and convert it to int
    df['tempmin'] = df['tempmin'].str.extract('(\d+)').astype(float)

    # Extract the numeric part of the temperature string and convert it to int
    df['tempmax'] = df['tempmax'].str.extract('(\d+)').astype(float)

    # Split wind speed values into components and convert speed to int type
    df['Wind Speed'] = df['Wind Speed'].str.extract('(\d+)', expand=False).fillna(0).astype(float)

    # Convert 'humidity' column to int type
    df['Humidity'] = df['Humidity'].str.extract('(\d+)', expand=False).astype(float)

    # Convert 'barometer' column to float type, and convert inches to millibars
    df['Barometer'] = round(df['Barometer'].apply(lambda x: float(x.split()[0]) * 33.8639 if 'in' in x and x != 'NA' else None), 2)

    # Convert 'Visibility' column to float type
    df['Visibility'] = df['Visibility'].str.extract('(\d+\.\d+|\d+)', expand=False).astype(float).round(2)

    #Convert 'last_update' column to UTC
    df['Last update'] = df['Last update'].apply(lambda x: dateutil.parser.parse(x, tzinfos={"EST": -5 * 3600, "CST": -6 * 3600, "MST": -7 * 3600,"PST": -8 * 3600,"AKST": -9 * 3600,"HST": -10 * 3600}))
    df['Last update'] = df['Last update'].apply(lambda x: x.astimezone(dateutil.tz.tzutc()))
    df['datetime'] = df['Last update'].dt.strftime('%Y-%m-%d')
    #df['datetime'] = pd.to_datetime(df['datetime'])

    # make wind chill a float if exists and only display degree F
    try:
        df[['Wind Chill']] = df['Wind Chill'].str.extract('(\d+)', expand=True).astype(float)
    except:
        None

    # extract the numeric value of dewpoint and only display the degree n farenheit
    df[['Dewpoint']] = df['Dewpoint'].str.extract('(\d+)', expand=True).astype(float)

    #change precip data type to float
    df['precip'] = df['precip'].astype(float)

    #rename weather station column to the city
    def rename_station(value):
        if value == 'Portland, Portland International Airport (KPDX)':
            return 'PORTLAND INTERNATIONAL AIRPORT, OR US'
        elif value == 'Sitka - Sitka Airport (PASI)':
            return 'SITKA AIRPORT, AK US'
        elif value == 'Philadelphia, Philadelphia International Airport (KPHL)':
            return 'PHILADELPHIA INTERNATIONAL AIRPORT, PA US'
        elif value == 'Orlando International Airport (KMCO)':
            return 'ORLANDO EXECUTIVE AIRPORT, FL US'
        elif value == 'Milwaukee, General Mitchell International Airport (KMKE)':
            return 'MILWAUKEE MITCHELL AIRPORT, WI US'
        elif value == 'Lynchburg, Lynchburg Regional Airport (KLYH)':
            return 'LYNCHBURG REGIONAL AIRPORT, VA US'
        elif value == 'Kalispell, Glacier Park International Airport (KGPI)':
            return 'KALISPELL GLACIER AIRPORT, MT US'
        elif value == 'Kahului, Kahului Airport (PHOG)':
            return 'KAHULUI AIRPORT, HI US'
        elif value == 'Fairbanks, Fairbanks International Airport (PAFA)':
            return 'FAIRBANKS INTERNATIONAL AIRPORT, AK US'
        elif value == 'Albuquerque, Albuquerque International Airport (KABQ)':
            return 'ALBUQUERQUE INTERNATIONAL AIRPORT, NM US'
        elif value == 'Bangor, Bangor International Airport (KBGR)':
            return 'BANGOR INTERNATIONAL AIRPORT, ME US'

    df['name'] = df['weather_station'].map(rename_station)

    #change the names and order of columns to better fit the historical data
    df = df.rename({'Humidity': 'humidity', 'Wind Speed': 'windspeed', 'Visibility': 'visibility','Wind Chill': 'windchill','Dewpoint':'dewpoint','tempmax':'temp_max','tempmin':'temp_min'}, axis=1) 
    #this line only includes necesarry columns
    df = df[['name','datetime','precip','temp_max','temp_min','temp','windchill','dewpoint','humidity','windspeed','visibility']]
    df = df.fillna(0)
    df = df.set_index(['datetime'])
    df.index.names = ['DATE']
    df.index = df.index.astype("string")

    source_dict = df.to_dict()
    
    return source_dict

# @task
# def create_test_set_forecast(dict1):
#     #create dataframes from the dicts since dicts can not be passed as xcoms
#     test_set_forecast = pd.DataFrame.from_dict(dict1)
#     # df2 = pd.DataFrame.from_dict(dict2)

#     #combine source df and test set so the test set had the most current info
#     # test_set_forecast = pd.concat([df1, df2])
#     #map each citiy to a numerical values to be used in the regression analysis
#     test_set_forecast['name'] = test_set_forecast['name'].astype('category')
#     test_set_forecast['city_number'] = test_set_forecast['name'].cat.codes
#     test_set_forecast = test_set_forecast[['name','precip','temp_max','temp_min','city_number']]

#     test_set_forecast_dict = test_set_forecast.to_dict()

#     return test_set_forecast_dict



@task
def create_10_day_forecast(core_weather_dict, test_set_forecast_dict):

    os.system('pip install scikit-learn')
    from sklearn.linear_model import Ridge

    regression = Ridge(alpha=.1)
    predictors = ['precip','temp_max','temp_min']

    test_set_forecast = pd.DataFrame.from_dict(test_set_forecast_dict)
    core_weather_df = pd.DataFrame.from_dict(core_weather_dict)

    training_set = core_weather_df.loc[:'2021-12-31']
    test_set_forecast = test_set_forecast[['name','precip','temp_max','temp_min']]
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_1'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([test_set_forecast[['name','temp_max']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1']

    regression.fit(training_set[predictors], training_set['target_temp_max_day_2'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2']

    regression.fit(training_set[predictors], training_set['target_temp_max_day_3'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3']

    regression.fit(training_set[predictors], training_set['target_temp_max_day_4'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_5'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_6'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_7'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_8'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7','Predicted_high_temp_day_8']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_9'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7','Predicted_high_temp_day_8']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7','Predicted_high_temp_day_8','Predicted_high_temp_day_9']
    
    regression.fit(training_set[predictors], training_set['target_temp_max_day_10'])
    predictions = regression.predict(test_set_forecast[predictors])
    combined_df = pd.concat([combined_df[['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7','Predicted_high_temp_day_8','Predicted_high_temp_day_9']], pd.Series(predictions, index = test_set_forecast.index)], axis=1)
    combined_df.columns = ['name','current_day_max_temp','Predicted_high_temp_day_1','Predicted_high_temp_day_2','Predicted_high_temp_day_3','Predicted_high_temp_day_4','Predicted_high_temp_day_5','Predicted_high_temp_day_6','Predicted_high_temp_day_7','Predicted_high_temp_day_8','Predicted_high_temp_day_9','Predicted_high_temp_day_10']
    

    combined_df = combined_df.astype({'current_day_max_temp':int,'Predicted_high_temp_day_1':int,'Predicted_high_temp_day_2':int,'Predicted_high_temp_day_3':int,'Predicted_high_temp_day_4':int,'Predicted_high_temp_day_5':int,'Predicted_high_temp_day_6':int,'Predicted_high_temp_day_7':int,'Predicted_high_temp_day_8':int,'Predicted_high_temp_day_9':int,'Predicted_high_temp_day_10':int})

    # combined_df = combined_df.iloc[-11:]
    ten_day_forecast_dict = combined_df.to_dict()

    return ten_day_forecast_dict

@task
def load_forecast_to_bigquery(data):

    PROJECT_ID = "deb-01-372116"
    DATASET_ID = "Weather_Forecaster"
    DAILY_TABLE_ID = "ten_day_forecast"

    SCHEMA = [
                # indexes are written if only named in the schema
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('current_day_max_temp', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_1', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_2', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_3', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_4', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_5', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_6', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_7', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_8', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_9', 'INT64', mode='NULLABLE'),
                bigquery.SchemaField('Predicted_high_temp_day_10', 'INT64', mode='REQUIRED'),
            ]

    #change the date column back to a datetime dtype
    df = pd.DataFrame(data)

    client = bigquery.Client()

    try:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = client.get_dataset(dataset_ref)
    except:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        # dataset.location = "US"
        dataset = client.create_dataset(dataset)

    table_ref = dataset.table(DAILY_TABLE_ID)

    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


@dag(
    schedule_interval="55 7 * * *",
    # schedule=timedelta(days=1),
    start_date=datetime.utcnow(),
    #start_date=pendulum.datetime(2023, 2, 19, 7, 55, tz="UTC"),
    catchup=True,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['dsa', 'dsa-example'],
)

def weather_forecaster():

    #read the csv from a downloaded dataset from NOAA
    read_historical_weather_data_task = read_historical_weather_data()

    #create the Bigquery client
    load_historical_data_to_bigquery_task = load_historical_data_to_bigquery(read_historical_weather_data_task)

    
    #scrape precip data
    scrape_precip_data_task= scrape_precip_data()
    #scrape nws data
    scrape_nws_data_task= scrape_nws_data()

    #combines the scraped data
    combine_scraped_dataframes_task = combine_scraped_dataframes(scrape_nws_data_task,scrape_precip_data_task)

    #transforms scraped data
    transform_scraped_data_task = transform_scraped_data(combine_scraped_dataframes_task)

    #create df used for the forecast
    # create_test_set_forecast_task = create_test_set_forecast(
    # transform_scraped_data_task)
    # #create_test_data_set_task,

    create_10_day_forecast_task = create_10_day_forecast(read_historical_weather_data_task, transform_scraped_data_task)

    #load forecast to bigQuery
    load_forecast_to_bigquery_task = load_forecast_to_bigquery(create_10_day_forecast_task)


    read_historical_weather_data_task >> load_historical_data_to_bigquery_task, [scrape_nws_data_task, scrape_precip_data_task] >> combine_scraped_dataframes_task >> transform_scraped_data_task >> create_10_day_forecast_task >> load_forecast_to_bigquery_task 

    # [scrape_nws_data_task, scrape_precip_data_task] >> combine_scraped_dataframes_task >> transform_scraped_data_task, read_historical_weather_data_task >> load_historical_data_to_bigquery_task, create_10_day_forecast_task >> load_forecast_to_bigquery_task 

dag = weather_forecaster()

