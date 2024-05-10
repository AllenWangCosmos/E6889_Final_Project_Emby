from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, ArrayType, LongType
from pyspark.streaming import StreamingContext
import time
from pyspark.sql.functions import to_timestamp, col, current_timestamp, date_sub, lit, unix_timestamp, expr, regexp_extract, when, regexp_replace, window, array, explode, first, count, split, size

import pandas as pd
import csv
from streamSubFunctions import *
from collections import Counter
import threading
import os
from pyspark.ml.fpm import FPGrowth

def file_watchdog(file_path):
    # Monitor the file for changes
    file_modification_time = os.path.getmtime(file_path)
    while True:
        current_modification_time = os.path.getmtime(file_path)
        if current_modification_time != file_modification_time:
            # File has been modified
            print("Triggered!")
            next_file_name = get_next_file_name('./data/')
            data = call_emby_api(emby_date_shifted())
            save_json_to_file(data, next_file_name)
            file_modification_time = current_modification_time
        time.sleep(1)  # Check every second



def emby_monitor():
    while True:
        time.sleep(60)  # Check every 60 seconds
        next_file_name = get_next_file_name('./data/')
        data = call_emby_api(emby_date_shifted())
        save_json_to_file(data, next_file_name)

       
#------------------------------------------------------------#
#------------------Function 1: Statistics--------------------#
#----------Spark Dependent Functions Definitions-------------#
#------------------------------------------------------------#

# Define the function for data-preprocessing and data_struct_conversion
# Define the function for data-preprocessing and data_struct conversion
def window_filtering(previous_stored_list, new_batch_df):
    # Convert the previous stored list to DataFrame
    if previous_stored_list:
        previous_stored_df = spark.createDataFrame(previous_stored_list)
    else:
        # If no data is present initially, create an empty DataFrame with expected schema
        schema = new_batch_df.schema  # Assuming new_batch_df has the complete schema
        previous_stored_df = spark.createDataFrame([], schema)
    
    # Drop unnecessary columns from new batch DataFrame
    required_columns = [col for col in previous_stored_df.columns if col in new_batch_df.columns]
    new_batch_df = new_batch_df.select(*required_columns)
    
    # Ensure both DataFrames have the same schema by ordering the columns
    previous_stored_df = previous_stored_df.select(*required_columns)
    new_batch_df = new_batch_df.select(*required_columns)

    # Union the stored DataFrame and new batch DataFrame
    union_df = previous_stored_df.unionByName(new_batch_df)
    
    # Apply time range filtering
    time_range = "month"  # This could be "week", "month", or "half_year"
    start_date = compute_start_date(time_range)
    
    # Filter the DataFrame based on the computed start date
    df_time_filtered = union_df.filter(col("Date") >= start_date)
    return df_time_filtered
#---------------------------End------------------------------#
#------------------Function 1: Statistics--------------------#
#----------Spark Dependent Functions Definitions-------------#
#------------------------------------------------------------#

#------------------------------------------------------------#
#------------------Function 1: Statistics--------------------#
#----------------Main Functions Definitions------------------#
#------------------------------------------------------------#
#  Read the initial overall history data from Emby server
def Statistics(batch_df, Id):
    global stored_list
    
    print("The following is the update for batch_id")
    print(Id)
    
    if Id == 0:
        time_range = "month"  # This could be "week", "month", or "half_year"

        start_date = compute_start_date(time_range)
        
        # Filter the DataFrame based on the computed start date
        df_time_filtered = batch_df.filter(col("Date") >= start_date)
        
        # Update the global variable
        stored_list = dict_stored_update(df_time_filtered)
        
        # Conduct initial statistical processing
        movie_names, series_names, counts_movie, counts_tv = process_media_data(df_time_filtered)
        
        # Translate the titles
        tv_series_titles, movie_titles = media_name_translate(series_names, movie_names)
        
        # Plot the results
        plot_media_views(movie_titles, counts_movie, tv_series_titles, counts_tv)

    else:
        # Apply sliding window for a time interval of a month
        df_time_filtered = window_filtering(stored_list, batch_df)
        
        # Update the global variable
        stored_list = dict_stored_update(df_time_filtered)
        
        # Conduct initial statistical processing
        movie_names, series_names, counts_movie, counts_tv = process_media_data(df_time_filtered)
        
        # Translate the titles
        tv_series_titles, movie_titles = media_name_translate(series_names, movie_names)
        
        # Plot the results
        plot_media_views(movie_titles, counts_movie, tv_series_titles, counts_tv)
        
#------------------------------------------------------------#
#----------------------End of Function 1---------------------#
#------------------------------------------------------------#

#------------------------------------------------------------#
#-----------------Function 2: Recommendation-----------------#
#------------------------------------------------------------#
def recommendations(batch_df):
    ########## Further Filtering ##########
    # Target user 19 for demo, user 1 for testing
    user_id = "1" 
    df_user_specific = batch_df.filter(col("UserId") == user_id)

    # Separate TV sereis and Movies
    series_name_pattern = r"^(.*?)\s*-\s*"
    series_name_df = df_user_specific.withColumn(
    "MediaName",
    when(col("MediaName").rlike(series_name_pattern), regexp_extract(col("MediaName"), series_name_pattern, 1))
    )
    series_name_df = series_name_df.filter(col("MediaName").isNotNull())
    movie_name_df = df_user_specific.filter(~col("MediaName").rlike(series_name_pattern))
    movie_name_df = movie_name_df.filter(col("MediaName").isNotNull())

    # Count by CN_MediaName to deduplicate
    series_name_merged = series_name_df.groupBy("MediaName").count()
    movie_name_merged = movie_name_df.groupBy("MediaName").count()
    movie_names_list = movie_name_merged.select("MediaName").rdd.flatMap(lambda x: x).collect()
    series_names_list = series_name_merged.select("MediaName").rdd.flatMap(lambda x: x).collect()

    ########## Find Genres by Media Name ##########

    # Initialize empty array for genres data storage
    tv_series_data = []
    movie_data = []

    # Get EN_MediaName and find the genres
    for name in series_names_list:
        # Call searchItem function
        search_result = search_item(name)
        if search_result:
            # Extract information
            genres = ", ".join(search_result["genres"])
            title = search_result["title"]
            tv_series_data.append([title, genres])
    
    for name in movie_names_list:
        # Call searchItem function
        search_result = search_item(name)
        if search_result:
            # Extract information
            genres = ", ".join(search_result["genres"])
            title = search_result["title"]
            movie_data.append([title, genres])

    ########## Read History Data ##########
    # Create batch df from the array 
    latest_series_df = spark.createDataFrame(tv_series_data, ["Title", "Genres"])
    series_csv_file_path = "./recommendations/series_genres.csv"
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Genres", StringType(), True)
    ])
    
    try:
        # Try to read CSV file
        history_series_genres_df = spark.read.csv(series_csv_file_path, schema=schema, header=True)

    except:
        # If file doesn't exist, create empty DataFrame
        history_series_genres_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    ########## Append New Dataframes with History for Series ##########
    combined_series_df = history_series_genres_df.union(latest_series_df)
    deduplicated_series_df = combined_series_df.groupBy("Title").agg({col: "first" for col in combined_series_df.columns})
    deduplicated_series_df = deduplicated_series_df.withColumnRenamed("first(Genres)", "Genres").drop("first(Title)")
    pd_deduplicated_series_df = deduplicated_series_df.toPandas()
    
    # Store current dataframe as history
    pd_deduplicated_series_df.to_csv(series_csv_file_path, index=False, encoding='utf_8_sig')
    deduplicated_series_df = deduplicated_series_df.withColumn("GenresArray", split(deduplicated_series_df["Genres"], ", "))

    ########## Start Association Rules Predicting for Series ##########
    fpGrowth = FPGrowth(itemsCol="GenresArray", minSupport=0.1, minConfidence=0.2)
    model = fpGrowth.fit(deduplicated_series_df)
    model.transform(deduplicated_series_df).show()
    predicted_df = model.transform(deduplicated_series_df).select("GenresArray", "prediction")
    exploded_series_genres_df = predicted_df.filter(size(col("prediction")) > 0).select(explode(col("prediction")).alias("Genres"))
    series_prediction_list = exploded_series_genres_df.select("Genres").rdd.map(lambda x: x[0]).collect()

    ########## Count Genres ##########
    # Flatten the list of genres
    movie_df = pd.DataFrame(movie_data, columns=["Title", "Genres"])
    flattened_movie_genres = movie_df['Genres'].str.split(', ').explode()

    # Count the occurrences of each genre
    flattened_series_genres = pd.DataFrame(series_prediction_list, columns=['Genres'])
    series_genres_counts = flattened_series_genres.value_counts().reset_index()
    series_genres_counts.columns = ['Genres', 'Counts']
    movie_genres_counts = flattened_movie_genres.value_counts().reset_index()
    movie_genres_counts.columns = ['Genres', 'Counts']

    ########## Aggregate Latest with History for Movies ##########
    # Read history csvs if it exists, otherwise create an empty DataFrame
    try:
        existing_movie_genres_counts = pd.read_csv('movieGenresCount.csv')
    except FileNotFoundError:
        existing_movie_genres_counts = pd.DataFrame(columns=['Genres', 'Counts'])
        
    # Append the latest counts to the existing DataFrame
    merged_movie_genre_counts = pd.concat([existing_movie_genres_counts, movie_genres_counts])
    # Group by genres column and aggregate counts, save all user played movies into csvs
    agged_series_genre_counts = series_genres_counts.groupby('Genres').agg({'Counts': 'sum'}).reset_index()
    agged_movie_genre_counts = merged_movie_genre_counts.groupby('Genres').agg({'Counts': 'sum'}).reset_index()
    ordered_series_genre_counts = agged_series_genre_counts.sort_values(by='Counts', ascending=False)
    ordered_movie_genre_counts = agged_movie_genre_counts.sort_values(by='Counts', ascending=False)
    ordered_movie_genre_counts.to_csv('./recommendations/movieGenresCount.csv', index=False)

    ########## Search Media by Genres and Give Recommendations in csv ##########
    # Naive Recommendation for Movie, association of rules for series
    # Get top 3 genres for movie and series
    top3_series_genres = ordered_series_genre_counts.head(3)
    top3_movie_genres = ordered_movie_genre_counts.head(3)
    top3_series_genres_names = top3_series_genres['Genres'].tolist()
    top3_movie_genres_names = top3_movie_genres['Genres'].tolist()

    # Match the genres with their ids from local lookup table
    genres_df = pd.read_csv('genres.csv')
    top3_series_genres_ids = genres_df.loc[genres_df['name'].isin(top3_series_genres_names), 'id'].tolist()
    top3_movie_genres_ids = genres_df.loc[genres_df['name'].isin(top3_movie_genres_names), 'id'].tolist()

    # Search media by each genres id, count occurances of media in every id.
    series_names_counts = Counter()
    movie_names_counts = Counter()
    
    # Iterate over each genre ID, not a df so can't aggregate
    for genre_id in top3_series_genres_ids:
        # Call the function to find movies by genre
        series_names = find_series_by_genre(genre_id)
        
        # Update the counts
        series_names_counts.update(series_names)
    
    for genre_id in top3_movie_genres_ids:
        # Call the function to find movies by genre
        movie_names = find_movies_by_genre(genre_id)
        
        # Update the counts
        movie_names_counts.update(movie_names)

    # Convert the media names and counts to a dictionary, get top 3 recommendations
    series_names_counts_dict = dict(series_names_counts)
    movie_names_counts_dict = dict(movie_names_counts)
    top3_series_names_counts = series_names_counts.most_common(3)
    top3_movie_names_counts = movie_names_counts.most_common(3)
    
    # Initialize empty array to store the information of top 3 media names
    top3_series_info = []
    top3_movie_info = []
    
    # Search Genres by Media Again to Give Recommendations
    for name, count in top3_series_names_counts:
        # Call the function to search for the item
        search_result = search_item(name)
        
        # If search result is not None, append the information to the array
        if search_result:
            top3_series_info.append({
                "genres": search_result["genres"],
                "title": search_result["title"]
            })
    
    for name, count in top3_movie_names_counts:
        # Call the function to search for the item
        search_result = search_item(name)
        
        # If search result is not None, append the information to the array
        if search_result:
            top3_movie_info.append({
                "genres": search_result["genres"],
                "title": search_result["title"]
            })
    
    # Create pandas df to save csv
    df_top3_series = pd.DataFrame(top3_series_info)
    df_top3_movie = pd.DataFrame(top3_movie_info)

    series_recom_csv_file_path = "./recommendations/seriesRecommend.csv"
    movie_recom_csv_file_path = "./recommendations/movieRecommend.csv"

    df_top3_series.to_csv(series_recom_csv_file_path, index=False, encoding='utf_8_sig')
    df_top3_movie.to_csv(movie_recom_csv_file_path, index=False, encoding='utf_8_sig')

    # Below for debugging only, if find Titles with empty genres, fill them on emby
    # movie_csv_file_path = "./recommendations/movie_genres.csv"
    # movie_df.to_csv(movie_csv_file_path, index=False, encoding='utf_8_sig')


    return 0

#------------------------------------------------------------#
#----------------------End of Function 2---------------------#
#------------------------------------------------------------#

#------------------------------------------------------------#
#---------Function 3: Streaming Triggered Function-----------#
#------------------------------------------------------------#
# Global varialbes
latest_processed_id = 0
# Stream DataFrames post processing
def post_processing(batch_df, batch_id):
    #try:
    global latest_processed_id
    record_count = batch_df.count()
    print(f"Batch {batch_id} has {record_count} records.")
    filtered_df = batch_df.filter(batch_df["Id"] > latest_processed_id)

        # Count the number of deleted rows
    latest_processed_id = batch_df.select("Id").first()[0]
    print("Latest processed ID:", latest_processed_id)
    filtered_df.show()
    if filtered_df.count()>0:
        Statistics(filtered_df, batch_id)
        recommendations(filtered_df)
        
    #except Exception as e:
       # print(f"Error processing batch {batch_id}: {str(e)}")

#------------------------------------------------------------#
#----------------------End of Function 3---------------------#
#------------------------------------------------------------#

########## Start Initialization ##########
# Data initialization, clean the history data and start from scratch
history_cleaner()

# Retrieve all Emby history from server for initialization
data_init = call_emby_api()
save_json_to_file(data_init, './data/emby_data_00.json')

# Initialize Spark session
spark = SparkSession.builder.appName("EmbyStreamProcessing").getOrCreate()
spark.conf.set("spark.sql.streaming.schemaInterference", True)

########## Start Filtering ##########
# Read TV channel names to exclude from analysis
channel_df = spark.read.csv("channel_names.csv", header=True, inferSchema=True)
channel_names = [row["Channel Name"] for row in channel_df.collect()]

# Define json schema based on Emby Format
schema = StructType([StructField('Items',ArrayType(StructType([StructField('Id',LongType(),True),
StructField('Name',StringType(),True),
StructField('ShortOverview',StringType(),True),
StructField('Type',StringType(),True),
StructField('Date',StringType(),True),
StructField('UserId',StringType(),True),
StructField('UserPrimaryImageTag',StringType(),True),
StructField('Severity',StringType(),True)])),True),
StructField('TotalRecordCount',IntegerType(),True)])

# Read the JSON file into stream DFs with the specified schema
json_df = spark.readStream.format("json").option("cleanSource","delete").option("multiline", "true").schema(schema).load("./data")

# Format the json
exploded_df = json_df.withColumn("ItemsListed", explode("Items"))
flattened_df = (
    exploded_df
    .drop("Items", "TotalRecordCount")
    .withColumn("Id", col("ItemsListed.Id"))
    .withColumn("Name", col("ItemsListed.Name"))
    .withColumn("Type", col("ItemsListed.Type"))
    .withColumn("Date", col("ItemsListed.Date"))
    .withColumn("UserId", col("ItemsListed.UserId"))
)

# Filter out other types of data
types_of_interest = ["VideoPlayback"]
filtered_df = flattened_df.filter(col("Type").isin(types_of_interest))

# Filter out TV channels
channels_filtered_df = filtered_df.filter(~col("Name").rlike("|".join(channel_names)))

# Use regexp_extract to extract the media name
with_media_name_df = channels_filtered_df.withColumn(
    "MediaName",
    regexp_extract(col("Name"), r"playing\s+(.*?)\s+on", 1)
)

# Filter out trash data
with_media_name_df = with_media_name_df.filter(col("MediaName") != "")
with_media_name_df = with_media_name_df.filter(col("MediaName").isNotNull())

########## Threads Online ##########
# Register threads to keep updating emby stream, and watch dog for manual trigger
trigger_file_path = "trigger_file.txt"
file_watchdog_thread = threading.Thread(target=file_watchdog, args=(trigger_file_path,))
emby_monitor_thread = threading.Thread(target=emby_monitor)
#file_watchdog_thread.start() # Enable Manual Trigger only for less frequent streaming (e.g. once a day)
emby_monitor_thread.start()

########## Start Streaming ##########
# Put everything into stream, call post_processing(batch_df, batch_id) when new stream coming in
stream = with_media_name_df.writeStream.outputMode("update").foreachBatch(post_processing).start()
stream.awaitTermination()