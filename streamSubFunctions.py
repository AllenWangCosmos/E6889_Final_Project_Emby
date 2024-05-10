import requests, json
from urllib.parse import urlencode
import csv
import os
from pyspark.sql.functions import to_timestamp, col, current_timestamp, date_sub, lit, unix_timestamp, expr, when, regexp_extract
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib import cm
import numpy as np

#------------------------------------------------------------#
#-------------Function Set 1: File Management----------------#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#
# Global varialbes initialize
file_count = 1
# Renaming json file to trigger spark stream
def get_next_file_name(directory='./data/', prefix='emby_data', extension='.json'):
    global file_count  # Access the global variable
    
    # Increment file count
    file_count_str = str(file_count).zfill(2)  # Pad with leading zeros if necessary
    file_count += 1
    
    # Construct the next file name
    next_file_name = f"{prefix}_{file_count_str}{extension}"
    next_file_path = os.path.join(directory, next_file_name)

    return next_file_path

# Start from scratch every time
def history_cleaner():
    series_csv_file_path = './recommendations/seriesGenresCount.csv'
    movie_csv_file_path = './recommendations/movieGenresCount.csv'
    association_history_path = './recommendations/series_genres.csv'
    json_path = './data'
    
    if os.path.exists(series_csv_file_path):
        os.remove(series_csv_file_path)
        print(f"File '{series_csv_file_path}' deleted successfully.")
    
    if os.path.exists(association_history_path):
        os.remove(association_history_path)
        print(f"File '{association_history_path}' deleted successfully.")
    
    if os.path.exists(movie_csv_file_path):
        os.remove(movie_csv_file_path)
        print(f"File '{movie_csv_file_path}' deleted successfully.") 

    files = os.listdir(json_path)

    # Iterate over each file
    for file in files:
        # Check if the file is a JSON file
        if file.endswith('.json'):
            # Construct the full file path
            file_path = os.path.join(json_path, file)
            # Delete the file
            os.remove(file_path)
            print(f"Deleted file: {file_path}")


def call_emby_api(date=None):
    EMBY_URL = 'https://gigasnow.synology.me:8921/emby/System/ActivityLog/Entries'
    API_KEY = '11012a4a9c684ad4862c63db2acb83fa'
    
    if date:
        ITEMS_URL = f'{EMBY_URL}?api_key={API_KEY}&MinDate={date}'
    else:
        ITEMS_URL = f'{EMBY_URL}?api_key={API_KEY}'
        
    response = requests.get(ITEMS_URL)
    
    if response.status_code == 200:
        data = response.json()
        items = data.get("Items", [])
        print("Emby fetched")
        return data
    else:
        return []

def save_json_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=0)  # Write JSON data to file with indentation

def emby_date_shifted():
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)
    formatted_date = two_days_ago.strftime("%Y-%m-%d")
    return formatted_date

#---------------------------End------------------------------#
#-------------Function set 1: File Management----------------#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#



#------------------------------------------------------------#
#----------------Function Set 2: Emby APIs-------------------#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#

def get_movie_name(tmdb_id):
    # Check if English names file exists
    english_names_file = "movieEnglishName.csv"
    if os.path.exists(english_names_file):
        # Check if tmdb_id exists in the English names file
        with open(english_names_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['tmdb_id'] == str(tmdb_id):
                    return row['title']

    # Construct the TMDB API URL
    tmdb_api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key=b00a7e603d0290b2cf716cf341f382ea"

    # Send the GET request to TMDB API
    tmdb_response = requests.get(tmdb_api_url)

    # Check if the request was successful (status code 200)
    if tmdb_response.status_code != 200:
        # Return an error message
        return {"error": "Failed to get TMDB result"}

    # Extract the title from the response JSON
    tmdb_response_json = tmdb_response.json()
    title = tmdb_response_json.get('title', "")

    # Append the id and title to the English names file
    with open(english_names_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([tmdb_id, title])

    # Return the title
    return title

def get_tv_name(tmdb_id):
    # Check if English names file exists
    english_names_file = "TVEnglishName.csv"
    if os.path.exists(english_names_file):
        # Check if tmdb_id exists in the English names file
        with open(english_names_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['tmdb_id'] == str(tmdb_id):
                    return row['title']

    # Construct the TMDB API URL
    tmdb_api_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key=b00a7e603d0290b2cf716cf341f382ea"

    # Send the GET request to TMDB API
    tmdb_response = requests.get(tmdb_api_url)

    # Check if the request was successful (status code 200)
    if tmdb_response.status_code != 200:
        # Return an error message
        print(f"tmdb failed id'{tmdb_id}' .")
        return {f"error": "Failed to get TMDB result for '{tmdb_id}'"}

    # Extract the title from the response JSON
    tmdb_response_json = tmdb_response.json()
    title = tmdb_response_json.get('name', "")

    # Append the id and title to the English names file
    with open(english_names_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([tmdb_id, title])

    # Return the title
    return title

def search_item(name_starts_with):
    url = "https://gigasnow.synology.me:8921/emby/Items"
    recursive = "true"
    api_key = "11012a4a9c684ad4862c63db2acb83fa"

    # Construct the parameters
    params = {
        "Recursive": recursive,
        "SearchTerm": name_starts_with,
        "api_key": api_key,
        "Fields": "Genres,ProviderIds"
    }

    # Construct the final request URL
    final_url = url + "?" + urlencode(params)

    # Send the GET request
    response = requests.get(final_url)

    # Check if the request was successful (status code 200)
    if response.status_code != 200:
        # Return an error message
        return {"error": response.text}
    
    emby_response_json = response.json()
    filtered_items = [item for item in emby_response_json["Items"] if item["Type"] in ["Series", "Movie"]]

    # Check if there are any filtered items
    if filtered_items:
        # Choose the first filtered item
        chosen_item = filtered_items[0]
    else:
        return {"error": "No items found with Type 'Series' or 'Movie'"}
    # Extract information from the response JSON
    genres = chosen_item.get("Genres", [])
    tmdb_id = chosen_item["ProviderIds"].get("Tmdb", "")
    type = chosen_item.get("Type", "")
    if not tmdb_id:
        return {"error": "Failed to get TMDb ID"}
    if type == "Movie":
        title = get_movie_name(tmdb_id)
    else:
        title = get_tv_name(tmdb_id)
    # Return the extracted information as JSON format
    return {
        "type": type,
        "genres": genres,
        "tmdb_id": tmdb_id,
        "title": title
    }

def find_movies_by_genre(genre_id):
    # Base URL
    base_url = "https://gigasnow.synology.me:8921/emby/Items"

    # Parameters
    params = {
        "IncludeItemTypes": "Movie,Series",
        "SortOrder": "Ascending",
        "IncludeItemTypes": "Movie",
        "GenreIds": genre_id,
        "Recursive": "true",
        "userId": "01167a66d7f34272b6d847fd34e4c07a",
        "X-Emby-Client": "Emby Web",
        "X-Emby-Device-Name": "Chromium macOS",
        "X-Emby-Device-Id": "dd7ce60e-6f44-4dad-aea7-46e4b29bbe7e",
        "X-Emby-Client-Version": "4.8.3.0",
        "api_key": "11012a4a9c684ad4862c63db2acb83fa",
        "X-Emby-Language": "en-gb",
        #"Limit": "2"
    }

    # Construct the final request URL
    url = base_url

    # Send the GET request
    response = requests.get(url, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code != 200:
        # Print the response content
        print(response.text)

    # Parse the JSON response
    emby_response_json = response.json()

    # Extract movie and series names
    names = [item["Name"] for item in emby_response_json["Items"]]
    
    return names
    
def find_series_by_genre(genre_id):
    # Base URL
    base_url = "https://gigasnow.synology.me:8921/emby/Items"

    # Parameters
    params = {
        "IncludeItemTypes": "Movie,Series",
        "SortOrder": "Ascending",
        "IncludeItemTypes": "Series",
        "GenreIds": genre_id,
        "Recursive": "true",
        "userId": "01167a66d7f34272b6d847fd34e4c07a",
        "X-Emby-Client": "Emby Web",
        "X-Emby-Device-Name": "Chromium macOS",
        "X-Emby-Device-Id": "dd7ce60e-6f44-4dad-aea7-46e4b29bbe7e",
        "X-Emby-Client-Version": "4.8.3.0",
        "api_key": "11012a4a9c684ad4862c63db2acb83fa",
        "X-Emby-Language": "en-gb",
    }

    # Construct the final request URL
    url = base_url

    # Send the GET request
    response = requests.get(url, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code != 200:
        # Print the response content
        print(response.text)

    # Parse the JSON response
    emby_response_json = response.json()

    # Extract movie and series names
    names = [item["Name"] for item in emby_response_json["Items"]]
    
    return names

#---------------------------End------------------------------#
#----------------Function Set 2: Emby APIs-------------------#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#



#------------------------------------------------------------#
#--Function set 3: Sub functions of main function Statistics-#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#
# Define the function to compute the start date for different time ranges
def compute_start_date(time_range):
    current_date = current_timestamp()
    return {
        "week": current_date - expr("INTERVAL 1 WEEK"),
        "month": current_date - expr("INTERVAL 1 MONTH"),
        "half_year": current_date - expr("INTERVAL 6 MONTH")
    }.get(time_range, current_date)

# Define the function to translate the CN array of string to EN
def media_name_translate(series_names_list, movie_names_list):
    """
    Fetch detailed information for media names from provided lists using a search function.
    Args:
    series_names_list (list): List of TV series names.
    movie_names_list (list): List of movie names.

    Returns:
    tuple: Two lists containing detailed titles for TV series and movies.
    """
    tv_series_data = []
    movie_data = []
    
    # Fetch details for TV series
    for name in series_names_list:
        search_result = search_item(name)
        if search_result:
            tv_series_data.append(search_result["title"])
    
    # Fetch details for movies
    for name in movie_names_list:
        search_result = search_item(name)
        if search_result:
            movie_data.append(search_result["title"])
    
    return tv_series_data, movie_data

# Define the function to plot the statistical bar chart
def plot_media_views(movie_data, counts_movie, tv_series_data, counts_tv, size=(15, 8), plot_titles=None):
    """
    Plots bar charts for the number of views per movie and TV series.
    
    Parameters:
        movie_data (list): List of movie names.
        counts_movie (list): Corresponding number of views for each movie.
        tv_series_data (list): List of TV series names.
        counts_tv (list): Corresponding number of views for each TV series.
        size (tuple): Figure size, default is (15, 8).
        plot_titles (tuple): A tuple containing two strings, the titles for the movie and TV series charts.
    """
    if plot_titles is None:
        plot_titles = ("Number of views per movie for all users in one month", 
                       "Number of views per TV series for all users in one month")
    
    # Set colors
    colors_movie = cm.rainbow(np.linspace(0, 1, len(movie_data)))
    colors_tv = cm.rainbow(np.linspace(0, 1, len(tv_series_data)))

    # Create figure
    plt.figure(figsize=size)

    # Plot the first bar chart
    plt.subplot(1, 2, 1)  # 1 row, 2 columns, position 1
    plt.bar(movie_data, counts_movie, color=colors_movie)
    plt.xlabel('Movie Name')
    plt.ylabel('Number of Views')
    plt.title(plot_titles[0])
    plt.xticks(rotation=45, ha='right')

    # Plot the second bar chart
    plt.subplot(1, 2, 2)  # 1 row, 2 columns, position 2
    plt.bar(tv_series_data, counts_tv, color=colors_tv)
    plt.xlabel('TV Series Name')
    plt.ylabel('Number of Views')
    plt.title(plot_titles[1])
    plt.xticks(rotation=45, ha='right')

    # Display the overall layout
    plt.tight_layout()
    plt.show()


# Define the function to seperate movie data and tv series data and do statistical processing
def process_media_data(df_time_filtered):
    # Pattern to separate TV series and Movies
    series_name_pattern = r"^(.*?)\s*-\s*"
    
    # Processing TV series names
    series_name_df = df_time_filtered.withColumn(
        "MediaName",
        when(col("MediaName").rlike(series_name_pattern), regexp_extract(col("MediaName"), series_name_pattern, 1))
    ).filter(col("MediaName").isNotNull())
    
    # Processing movie names
    movie_name_df = df_time_filtered.filter(~col("MediaName").rlike(series_name_pattern)).filter(col("MediaName").isNotNull())
    
    # Count by MediaName and order by count descending
    series_name_merged = series_name_df.groupBy("MediaName").count().orderBy(col("count").desc())
    movie_name_merged = movie_name_df.groupBy("MediaName").count().orderBy(col("count").desc())
    
    # Collecting names and counts into lists
    movie_names_list = movie_name_merged.select("MediaName").rdd.flatMap(lambda x: x).collect()
    series_names_list = series_name_merged.select("MediaName").rdd.flatMap(lambda x: x).collect()
    counts_movie = movie_name_merged.select("count").rdd.flatMap(lambda x: x).collect()
    counts_tv = series_name_merged.select("count").rdd.flatMap(lambda x: x).collect()

    # Return the processed data
    return movie_names_list, series_names_list, counts_movie, counts_tv

# Define the function for converting the dataframe back to list
def dict_stored_update(df_time_filtered):
    # Define the columns to retain
    retained_columns = ['Date', 'MediaName']

    # Filter the DataFrame to only include necessary columns
    filtered_df = df_time_filtered.select(*retained_columns)

    # Convert the filtered DataFrame back to a list of dictionaries
    new_stored_list = [row.asDict() for row in filtered_df.collect()]
    return new_stored_list

#---------------------------End------------------------------#
#--Function set 3: Sub functions of main function Statistics-#
#------------------Functions Definitions---------------------#
#------------------------------------------------------------#