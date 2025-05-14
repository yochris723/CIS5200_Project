# Databricks notebook source
import pandas as pd
import os
import glob

# COMMAND ----------

# Step 1: Specify the directory containing the CSV files
directory = '/Users/CLee/OneDrive - Cal State LA/CIS 5200/Project/Sephora Products and Skincare Reviews/Reviews'

# COMMAND ----------

# Step 2: Get a list of all CSV files in the directory
csv_files = glob.glob(os.path.join(directory, '*.csv'))

# COMMAND ----------

# Step 3: Initialize an empty list to store DataFrames
dataframes = []

# COMMAND ----------

# Step 4: Reach each CSV file and append to the list
for file in csv_files:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file, low_memory=False)
    
    # Append the DataFrame to the list
    dataframes.append(df)

# COMMAND ----------

# Step 5: Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# COMMAND ----------

#Step 6: Save the combined DataFrame to a new CSV file
combined_df.to_csv(os.path.join(directory, 'combined_sephorareviews.csv'), index=False)

print(f"Combined {len(csv_files)} CSV files into 'combined_sephorareviews.csv'")