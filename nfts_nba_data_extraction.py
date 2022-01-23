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
import subprocess

## Delete old moments data
now = time.time()

moments_folder = os.getcwd() + "\\moments_data"

files = [os.path.join(moments_folder, filename) for filename in os.listdir(moments_folder)]
for filename in files:
    if (now - os.stat(filename).st_mtime) > 60:
        try:
            os.remove(filename)
        except OSError:
            pass

## Download new moments' data
chrome_options = webdriver.ChromeOptions()

prefs = {'download.default_directory' : moments_folder}
chrome_options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(executable_path='chromedriver', options=chrome_options)

#driver = webdriver.Chrome(executable_path='chromedriver')
time.sleep(3)

driver.get('https://otmnft.com/moments/')#put here the adress of your page
moments_btn = driver.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/form/div[1]/div/div/div[3]/center/button[2]')
moments_btn.click()
time.sleep(4)

driver.close()

## Read the latest moments data
# Set the path for the latest moments data
latest_moments_path= moments_folder + '\\moments*.csv'

# Create dict specifying data types for Series and zipcode
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
moments_538 = merged_df.merge(merged_df_new,on="player_name")

moments_538['Low Ask'] = moments_538['Low Ask'].astype(int)

moments_538['cs_per_dollar'] = moments_538['Collector Score']/moments_538['Low Ask']

moments_538['cs_per_dollar'] = moments_538['cs_per_dollar'].round(2)

moments_538['market_cap']= moments_538['Circulation Count']*moments_538['Low Ask']

# Replace five_thirty_eight blanks with 'vintage'
moments_538['category'].fillna('vintage', inplace=True)

moments_538['age'].fillna('vintage', inplace=True)

moments_538['position'].fillna('vintage', inplace=True)

moments_538['market_value'].fillna('vintage', inplace=True)

## Delete old NBA Stats data
stats_folder = os.getcwd() + "\\stats_data"

files = [os.path.join(stats_folder, filename) for filename in os.listdir(stats_folder)]
for filename in files:
    if (now - os.stat(filename).st_mtime) > 60:
        try:
            os.remove(filename)
        except OSError:
            pass

## Download NBA Stats data
prefs = {'download.default_directory' : stats_folder}
chrome_options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(executable_path='chromedriver', options=chrome_options)

#driver = webdriver.Chrome(executable_path='chromedriver')
time.sleep(3)

driver.get('https://www.nbastuffer.com/2021-2022-nba-player-stats/') #put here the adress of your page
stats_btn = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[1]/div/div/article/div/div/div[3]/div/div[1]/button/span')
stats_btn.click()
time.sleep(4)

driver.close()

## Read the latest NBA Stats data
# Set the path for the latest stats data
latest_stats_path= stats_folder + '\\NBA*.xlsx'

# Create dict specifying data types for Series and zipcode
#data_types = {"Series": "category", "zipcode": str}

try:
    
    # Read the latest stats file
    list_of_stats_files = glob.glob(latest_stats_path) # * means all if need specific format then *.csv
    latest_stats_file = max(list_of_stats_files, key=os.path.getctime)
    
    cols2skip = [0,2,3,4]  
    cols = [i for i in range(29) if i not in cols2skip]
    
    # Read the CSV and assign it to the variable stats
    stats = pd.read_excel(latest_stats_file, skiprows=[0], usecols=cols, engine='openpyxl')
        
    #stats = stats[~stats.Set.str.contains("WNBA|In Her Bag")]

    stats = stats.rename(columns={'FULL NAME': 'full_name', 'MIN%Minutes PercentagePercentage of team minutes used by a player while he was on the floor': 'minutes_percentage', 'USG%Usage RateUsage rate, a.k.a., usage percentage is an estimate of the percentage of team plays used by a player while he was on the floor': 'usage_rate', 'TO%Turnover RateA metric that estimates the number of turnovers a player commits per 100 possessions': 'turnover_rate', 'eFG%Effective Shooting PercentageWith eFG%, three-point shots made are worth 50% more than two-point shots made. eFG% Formula=(FGM+ (0.5 x 3PM))/FGA': 'effective_shooting_percentage', 'TS%True Shooting PercentageTrue shooting percentage is a measure of shooting efficiency that takes into account field goals, 3-point field goals, and free throws.': 'true_shooting_percentage', 'PPGPointsPoints per game.': 'points_per_game', 'RPGReboundsRebounds per game.': 'rebounds_per_game', 'TRB%Total Rebound PercentageTotal rebound percentage is estimated percentage of available rebounds grabbed by the player while the player is on the court.': 'total_rebound_percentage', 'APGAssistsAssists per game.': 'assists_per_game', 'AST%Assist PercentageAssist percentage is an estimated percentage of teammate field goals a player assisted while the player is on the court': 'assist_percentage', 'SPGStealsSteals per game.': 'steals_per_game', 'BPGBlocksBlocks per game.': 'blocks_per_game', 'TOPGTurnoversTurnovers per game.': 'turnovers_per_game', 'VIVersatility IndexVersatility index is a metric that measures a player’s ability to produce in points, assists, and rebounds. The average player will score around a five on the index, while top players score above 10': 'versatility_index', 'ORTGOffensive RatingIndividual offensive rating is the number of points produced by a player per 100 total individual possessions.': 'offensive_rating', 'DRTGDefensive RatingIndividual defensive rating estimates how many points the player allowed per 100 possessions he individually faced while staying on the court.': 'defensive_rating'})
    
except pd.io.common.CParserError:
    print("Your data contained rows that could not be parsed.")

## Merge the latest NBA Stats data
stats.full_name = stats.full_name.map(lambda x: difflib.get_close_matches(x, moments_538.player_name, cutoff=0.8))

stats["full_name"] = stats["full_name"].str[0]

stats['full_name'] = stats['full_name'].astype(str)

# Merge five_thirty_eight into moments on the player_name
moments_538_stats = moments_538.merge(stats, how='right', left_on="player_name",right_on="full_name")

moments_538_stats = moments_538_stats[moments_538_stats['player_name'].notna()]
moments_538_stats = moments_538_stats.drop('full_name', axis=1)

accumulated_data_path = os.getcwd() + '\\accumulated_data\\merged_data.csv'

existing_data = pd.read_csv(accumulated_data_path)

refreshed_df= pd.concat([existing_data, moments_538_stats], axis=0, ignore_index=False)

refreshed_df = refreshed_df.drop_duplicates()

refreshed_df.to_csv(accumulated_data_path, sep=',', index=False)

end = time.time()
print('\n Success! Your moments data has been updated in', int(end - start), 'seconds.')