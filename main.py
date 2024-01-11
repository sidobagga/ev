import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import re
import numpy as np
from functools import reduce


# Constants
DATA_FOLDER = '/Users/sidbagga/Downloads/evchargers_census/Data/'
CSA_SHAPEFILE_PATH = '/Users/sidbagga/Downloads/cb_2018_us_csa_5m/cb_2018_us_csa_5m.shp'
BA_SHAPEFILE_PATH = "/Users/sidbagga/Downloads/Control__Areas/Control__Areas.shp"
EV_CHARGERS_PATH = DATA_FOLDER + 'Electric and Alternative Fuel Charging Stations.csv'
POPULATION_DATA_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/metro/totals/csa-est2022.csv"
ACS_DATA_PATH = DATA_FOLDER + 'ACS_Data.csv'


def read_csv_with_encoding(url, encodings=['utf-8', 'latin1', 'cp1252']):
    """Read CSV with multiple encodings and return a DataFrame."""
    for encoding in encodings:
        try:
            return pd.read_csv(url, encoding=encoding)
        except UnicodeDecodeError:
            print(f"Error with {encoding} encoding. Trying another.")

# Import Census Combined Statistical Area (CSA) Boundary Shapefile as a GeoDataFrame
metro_areas = gpd.read_file(CSA_SHAPEFILE_PATH)

metro_areas = metro_areas.to_crs(epsg=4326)

#Import Balancing Authority Boundary Shapefile as a GeoDataFrame
balancing_authorities = gpd.read_file(BA_SHAPEFILE_PATH)
balancing_authorities = balancing_authorities.to_crs(epsg=4326)

# NREL EV Charger Data
ev_chargers = pd.read_csv(EV_CHARGERS_PATH, parse_dates = ['Open Date'])

# Convert EV Charger DataFrame to a GeoDataFrame using lat/long columns
gdf_points = gpd.GeoDataFrame(ev_chargers, geometry=gpd.points_from_xy(ev_chargers['Longitude'], ev_chargers['Latitude']), crs='EPSG:4326')

# Join EV GeoDataFrame with CSA Boundaries
joined_gdf_a = gpd.sjoin(gdf_points, metro_areas, how='left', op='within').drop(columns = ['index_right'])
joined_gdf = gpd.sjoin(joined_gdf_a, balancing_authorities, how='left', op='within').drop(columns = ['index_right'])



# Filter DF down to only electricity fueling tech
joined_gdf_electric = joined_gdf[joined_gdf['Fuel Type Code'] == 'ELEC']
joined_gdf_electric.drop_duplicates(subset = 'ID_left', inplace = True)
joined_gdf_electric.rename(columns = {"NAME_left": "metro_name", "NAME_right":"ba_name"}, inplace = True)

# Return DF with EV Charger Locations by major metro area (CSA)
metro_areas_evchargers = joined_gdf_electric['metro_name'].value_counts().reset_index()
metro_areas_evchargers.columns = ['metro_name', 'count']

# Do the same for counts of Tesla Locations
teslacounts = joined_gdf_electric.query("`EV Connector Types`.fillna('').str.contains('TESLA')", engine = 'python')['metro_name'].value_counts().reset_index()

#Get a sum of counts for each metro areas
level1counts = joined_gdf_electric.groupby(['metro_name'])['EV Level1 EVSE Num'].sum()
#Level 1 Chargers take 8-10 hours to recharge a battery and are less than are less than 1% of public fleet; they will be excluded from analysis hereon
level2counts = joined_gdf_electric.groupby(['metro_name'])['EV Level2 EVSE Num'].sum()
fastchargecounts = joined_gdf_electric.groupby(['metro_name'])['EV DC Fast Count'].sum()

#Perform the merge using reduce
dataframes_to_merge = [metro_areas_evchargers, level2counts, fastchargecounts]
metro_areas_evchargers = reduce(lambda left, right: pd.merge(left, right, on='metro_name', how='left'), dataframes_to_merge)
metro_areas_evchargers['total_num'] = metro_areas_evchargers['EV Level2 EVSE Num'] + metro_areas_evchargers['EV DC Fast Count'] 


# Return DF with EV Charger Locations by Balancing Authority
control_areas_evchargers = joined_gdf_electric['ba_name'].value_counts().reset_index()

#Get a sum of counts for each balancingauthority
level1counts = joined_gdf_electric.groupby(['ba_name'])['EV Level1 EVSE Num'].sum()
#Level 1 Chargers take 8-10 hours to recharge a battery and are less than are less than 1% of public fleet; they will be excluded from analysis hereon
level2counts = joined_gdf_electric.groupby(['ba_name'])['EV Level2 EVSE Num'].sum()
fastchargecounts = joined_gdf_electric.groupby(['ba_name'])['EV DC Fast Count'].sum()


#Perform the merge using reduce
dataframes_to_merge = [control_areas_evchargers, level2counts, fastchargecounts]
control_areas_evchargers = reduce(lambda left, right: pd.merge(left, right, on='ba_name', how='left'), dataframes_to_merge)
control_areas_evchargers['total_num'] = control_areas_evchargers['EV Level2 EVSE Num'] + control_areas_evchargers['EV DC Fast Count'] 



# Return DF with EV Charger Locations by City
cities_evchargers = joined_gdf_electric['City'].value_counts().reset_index()
cities_evchargers.columns = ['City', 'count']
#Get a sum of counts for each city
level1counts = joined_gdf_electric.groupby(['City'])['EV Level1 EVSE Num'].sum()
#Level 1 Chargers take 8-10 hours to recharge a battery and are less than are less than 1% of public fleet; they will be excluded from analysis hereon
level2counts = joined_gdf_electric.groupby(['City'])['EV Level2 EVSE Num'].sum()
fastchargecounts = joined_gdf_electric.groupby(['City'])['EV DC Fast Count'].sum()


#Perform the merge using reduce
dataframes_to_merge = [cities_evchargers, level2counts, fastchargecounts]
cities_evchargers = reduce(lambda left, right: pd.merge(left, right, on='City', how='left'), dataframes_to_merge)
cities_evchargers['total_num'] = cities_evchargers['EV Level2 EVSE Num'] + cities_evchargers['EV DC Fast Count'] 





#Retrieve CSA id
metro_areas_evchargers = pd.merge(metro_areas_evchargers, metro_areas[['NAME', 'CSAFP']], left_on='metro_name', right_on='NAME').drop(columns=['NAME'])

# Import CSA Population data
populations = read_csv_with_encoding(POPULATION_DATA_URL)
populations['CSA'] = populations['CSA'].astype(str)

# Filter population DataFrame to CSA level
populations_bycsa = populations.query("LSAD == 'Combined Statistical Area'")

# Merge population DataFrame with EV charger count DataFrame
metroareas_evchargers_populations = pd.merge(metro_areas_evchargers, populations_bycsa, left_on="CSAFP", right_on  = "CSA").sort_values(by="ESTIMATESBASE2020", ascending=False)

# Merge with area columns from original metro_areas DataFrame
metroareas_evchargers_populations_area = (
    pd.merge(metro_areas_evchargers, metro_areas[['NAME', 'ALAND', 'geometry']], left_on='metro_name', right_on='NAME')
    .merge(populations_bycsa[['NAME', 'ESTIMATESBASE2020', 'POPESTIMATE2022']], on="NAME")
    .sort_values(by="ESTIMATESBASE2020", ascending=False).drop(columns=['NAME'])
)

# Return per-capita and per-area values for EV Chargers in each CSA (per 10k population, per 1000 sqkm)
metroareas_evchargers_populations_area['pertenthousandcapita'] = (
    metroareas_evchargers_populations_area['count'] / metroareas_evchargers_populations_area['ESTIMATESBASE2020'] * 10000
)
metroareas_evchargers_populations_area['squarekm'] = metroareas_evchargers_populations_area['ALAND'] / 1000000
metroareas_evchargers_populations_area['perthousandsqkm'] = (
    metroareas_evchargers_populations_area['count'] / metroareas_evchargers_populations_area['squarekm'] * 1000
)

# Import ACS Data
ACSdata = pd.read_csv(ACS_DATA_PATH)

# Merge ACS Data and clean column names
metroareas_evchargers_populations_area_ACS = pd.merge(metroareas_evchargers_populations_area, ACSdata, left_on= "metro_name", right_on= "NAME").rename(columns=str.strip).drop(columns=['NAME'])

# Import ACS Vehicles data
ACSvehicles = pd.read_csv("/Users/sidbagga/Downloads/evchargers_census/Data/ACS_aggvehiclesbytenure.csv")

#Merge and Clean
metroareas_evchargers_populations_area_ACS = pd.merge(metroareas_evchargers_populations_area_ACS, ACSvehicles, left_on = "metro_name", right_on = "NAME").rename(columns=str.strip).drop(columns=['NAME'])



def clean_column_name(col):
    col = col.lower()
    col = re.sub(r'\W', '_', col)  # Replace non-alphanumeric characters with underscores
    col = re.sub(r'_+', '_', col)  # Replace consecutive underscores with a single underscore
    
    # If the column starts with a number, add a prefix 'col_' to it
    if col[0].isdigit() and col in {'2020_or_later', '2010_to_2019', '2000_to_2009', '1980_to_1999', '1960_to_1979', '1940_to_1959', '1939_or_earlier'}:
        col = 'year_' + col
    elif col[0].isdigit():
        col = 'count_' + col
    
    return col

metroareas_evchargers_populations_area_ACS.columns = metroareas_evchargers_populations_area_ACS.columns.map(clean_column_name)






metroareas_evchargers_populations_area_ACS.to_csv(DATA_FOLDER + 'output_data.csv', index=False)


metro_areas_evchargers_overtime.to_csv(DATA_FOLDER + 'metro_overtime_data.csv', index=False)
