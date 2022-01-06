import time
start = time.time()

## Import libraries
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import os
# Import pandas as pd
import pandas as pd
import glob

## Download new moments' data
chrome_options = webdriver.ChromeOptions()

#os.getcwd()

moments_path = os.getcwd() + "\\moments_data"
#moments_Path

prefs = {'download.default_directory' : moments_path}
chrome_options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(executable_path='chromedriver', options=chrome_options)

#driver = webdriver.Chrome(executable_path='chromedriver')
time.sleep(3)

driver.get('https://otmnft.com/moments/')#put here the adress of your page
btn = driver.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/form/div[1]/div/div/div[3]/center/button[2]')
btn.click()
time.sleep(4)

driver.close()

## Read the latest moments data
# Set the path for the latest moments data
latest_moments_path= moments_path + '\\moments*.csv'

# Create dict specifying data types for agi_stub and zipcode
data_types = {"Series": "category", "zipcode": str}

date_columns = ['Time Stamp (EST)', 'Date of Moment']

try:
    
    # Read the latest moments file
    list_of_files = glob.glob(latest_moments_path) # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    
    # Read the CSV and assign it to the variable moments
    moments = pd.read_csv(latest_file, dtype= data_types, parse_dates=date_columns)
        
    moments = moments[~moments.Set.str.contains("WNBA|In Her Bag")]

    moments = moments.rename(columns={'Date of Moment': 'date_of_moment', 'Player Name': 'player_name'})

    moments['date_of_moment'] = pd.to_datetime(moments.date_of_moment).dt.tz_localize(None)
    
except pd.io.common.CParserError:
    print("Your data contained rows that could not be parsed.")

moments['player_name'] = moments['player_name'].str.replace('.', '', regex=True)

moments['Time Stamp (EST)'] = moments['Time Stamp (EST)'].dt.date

five_thirty_eight_path = os.getcwd() + "\\five_thirty_eight\\pcapmv.xlsx"

# Read spreadsheet and assign it to survey_responses
five_thirty_eight = pd.read_excel(five_thirty_eight_path, engine='openpyxl')

five_thirty_eight['player'] = five_thirty_eight['player'].str.title()

name_suffixes_replacement = {"Iii": "III", "Ii": "II"}

five_thirty_eight['player'] = five_thirty_eight['player'].replace(name_suffixes_replacement, regex=True)

five_thirty_eight['age'] = five_thirty_eight['age'].str.replace(' years old', '', regex=True)

market_value_replacement = {"\$": "", "m": ""}

five_thirty_eight['market_value'] = five_thirty_eight['market_value'].replace(market_value_replacement, regex=True)

import difflib

difflib.get_close_matches

five_thirty_eight.player = five_thirty_eight.player.map(lambda x: difflib.get_close_matches(x, moments.player_name, cutoff=0.8))

five_thirty_eight["player"] = five_thirty_eight["player"].str[0]

five_thirty_eight['player'] = five_thirty_eight['player'].astype(str)

# Merge five_thirty_eight into moments on the player_name
merged_df = five_thirty_eight.merge(moments, how='right', left_on="player",right_on="player_name")

merged_df = merged_df.drop('player', axis=1)

merged_df['moments_count'] = merged_df.groupby(['player_name'])['category'].transform('count')

merged_df_new= pd.crosstab(merged_df.player_name,merged_df.Series)

merged_df_new = merged_df_new.stack().reset_index().rename(columns={0:'series_count'})

merged_df_new['Series'] = merged_df_new['Series'].map({'1':'CS1', '2':'CS2', '3':'CS3', '4':'CS4'}) 

merged_df_new = merged_df_new.pivot_table(values='series_count', index=['player_name'], columns=['Series'], aggfunc='sum')

merged_df_new.fillna(0, inplace= True)

merged_df_new['player_name'] = merged_df_new.index

merged_df_new.reset_index(drop=True, inplace=True)

# Merge five_thirty_eight into moments on the player_name
final_df = merged_df.merge(merged_df_new,on="player_name")

final_df['Low Ask'] = final_df['Low Ask'].astype(int)

final_df['cs_per_dollar'] = final_df['Collector Score']/final_df['Low Ask']

final_df['cs_per_dollar'] = final_df['cs_per_dollar'].round(2)

final_df['market_cap']= final_df['Circulation Count']*final_df['Low Ask']

# Replace five_thirty_eight blanks with 'vintage'
final_df['category'].fillna('vintage', inplace=True)

final_df['age'].fillna('vintage', inplace=True)

final_df['position'].fillna('vintage', inplace=True)

final_df['market_value'].fillna('vintage', inplace=True)

accumulated_data_path = os.getcwd() + '\\accumulated_data\\merged_data.csv'

existing_data = pd.read_csv(accumulated_data_path)

refreshed_df= pd.concat([existing_data, final_df], axis=0, ignore_index=False)

refreshed_df.to_csv(accumulated_data_path, sep=',', index=False)

end = time.time()
print('Your moments data has been updated in', end - start, 'seconds')