import zipfile
import polars as pl
import json
import requests
import mysql.connector
from io import BytesIO
import os
import configparser

# Load credentials from properties file
config = configparser.ConfigParser()
config.read("config.properties")

# Read values
username = config.get("DEFAULT", "USERNAME")
password = config.get("DEFAULT", "PASSWORD")

# MySQL Database Connection
db_conn = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="Admin@123",
    database="parquetdb"
)
cursor = db_conn.cursor()

# Enter the latest and earlier dates respectively.
date1 = input("Enter the latest snapshot date (YYYY-MM-DD): ")
date2 = input("Enter the earlier snapshot date (YYYY-MM-DD): ")

# Paths for downloaded files
download_dir = "zip_file_downloads"
json_dir = "json_master"  # Directory for output JSON files
os.makedirs(download_dir, exist_ok=True)
os.makedirs(json_dir, exist_ok=True)

zip_path1 = os.path.join(download_dir, f"insights_{date1}.zip")
zip_path2 = os.path.join(download_dir, f"insights_{date2}.zip")


# Downloading the zip files automatically.
def download_zip(snapshot_date, save_path):
    url = f"https://tdgroup-dev.collibra.com/rest/2.0/reporting/insights/directDownload?snapshotDate={snapshot_date}&format=zip"
    response = requests.get(url, auth=(username, password), stream=True)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {save_path}")
    else:
        print(f"Failed to download {snapshot_date}: {response.status_code} - {response.text}")
        exit(1)


# Download the ZIP files
download_zip(date1, zip_path1)
download_zip(date2, zip_path2)


# 'extract_parquet_from_zip' function unzips and reads the parquet files in each folder.
def extract_parquet_from_zip(zip_path):
    extracted_files = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith(".parquet"):
                folder = file_name.split("/")[0]  # Extract folder name
                if folder not in extracted_files:
                    extracted_files[folder] = []
                extracted_files[folder].append((file_name, BytesIO(zip_ref.read(file_name))))
    return extracted_files


# 'compare_parquet_files' function compares the latest date file with earlier date file and creates dataframes.
def compare_parquet_files(df1, df2):
    if "edited_date" not in df1.columns or "edited_date" not in df2.columns:
        return {"Edited_date column is not present in one of the DataFrames"}

    # Convert edited_date to string type
    df1 = df1.with_columns(pl.col("edited_date").cast(pl.Utf8))
    df2 = df2.with_columns(pl.col("edited_date").cast(pl.Utf8))

    # Finding rows that exist in dataframe1 but not in dataframe2
    differences = df1.join(df2, on="edited_date", how="anti")

    # Converting all non-string columns to string
    differences = differences.with_columns(
        [pl.col(col).cast(pl.Utf8) for col in differences.columns]
    )

    # Group differences by "edited_date"
    grouped_differences = {}
    for row in differences.to_dicts():
        edited_date = row["edited_date"]  # Extracting the edited_date
        row["edited_date"] = edited_date  # Checking that it is present in each data entry

        if edited_date not in grouped_differences:
            grouped_differences[edited_date] = []
        grouped_differences[edited_date].append(row)

    # Formatting the result
    results = [{"edited_date": date, "data": data} for date, data in grouped_differences.items()]
    return results


# process_comparison function will return the output jsons for respective json folders. They are stored in json_master
def process_comparison(zip1, zip2, json_dir):
    files1 = extract_parquet_from_zip(zip1)
    files2 = extract_parquet_from_zip(zip2)

    # Ensuring that the output directory exists
    os.makedirs(json_dir, exist_ok=True)

    for folder in files1.keys():
        if folder in files2:
            folder_results = []
            for (file1_name, file1_content), (file2_name, file2_content) in zip(files1[folder], files2[folder]):
                df1 = pl.read_parquet(file1_content)
                df2 = pl.read_parquet(file2_content)
                diff = compare_parquet_files(df1, df2)
                if diff:
                    folder_results.append({
                        "file1": file1_name,
                        "file2": file2_name,
                        "differences": diff
                    })

            # Save the result for the folder in its own JSON file
            output_file = os.path.join(json_dir, f"{folder}.json")
            with open(output_file, "w") as f:
                json.dump(folder_results, f, indent=4, default=list)
            print(f"Comparison saved in {output_file}")


# Giving the final input here and running the process_comparison function
process_comparison(zip_path1, zip_path2, json_dir)

# Define the directory containing JSON files
json_directory = "json_master"

# Iterate over each JSON file
for filename in os.listdir(json_directory):
    if filename.endswith(".json"):
        file_path = os.path.join(json_directory, filename)

        with open(file_path, "r") as file:
            json_data = json.load(file)

            for entry in json_data:
                file1 = entry.get("file1")
                file2 = entry.get("file2")
                differences = entry.get("differences", [])

                for diff_entry in differences:
                    if isinstance(diff_entry, dict):  # Ensure it's a dictionary before accessing `.get()`
                        edited_date = diff_entry.get("edited_date")
                        data = json.dumps(diff_entry.get("data", []))  # Convert list to JSON string

                        # Insert into MySQL
                        sql = """
                        INSERT INTO insights_comparison (folder_name, file1, file2, edited_date, data)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        values = (filename.replace(".json", ""), file1, file2, edited_date, data)
                        cursor.execute(sql, values)
                    else:
                        print(f"Skipping invalid entry in {filename}: {diff_entry}")  # Debugging message

# Commit changes and close the connection
db_conn.commit()
cursor.close()
db_conn.close()

print("JSON data successfully inserted into MySQL database.")
