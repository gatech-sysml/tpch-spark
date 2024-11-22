import json
import os

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EventLogReader") \
    .getOrCreate()


# Function to read event logs and extract stage-wise information
def extract_stage_info(event_log_file):
    # Read event log file
    events_df = spark.read.format("binaryFile").load(event_log_file)
    
    # Filter only StageCompleted events
    stage_completed_events = events_df.filter(events_df['Event'] == 'SparkListenerStageCompleted')
    
    # Extract stage-wise information
    stage_info = stage_completed_events.select(
        "Stage Info.Stage ID",
        "Stage Info.Name",
        "Stage Info.Number of Tasks",
        "Stage Info.Submission Time",
        "Stage Info.Completion Time"
    ).collect()
    
    # Convert data to dictionary format
    stage_data = []
    for row in stage_info:
        stage_data.append({
            "Stage ID": row["Stage ID"],
            "Name": row["Name"],
            "Number of Tasks": row["Number of Tasks"],
            "Submission Time": row["Submission Time"],
            "Completion Time": row["Completion Time"]
        })
    
    return stage_data


# Function to process all event log files in a directory
def process_event_logs(log_directory):
    all_runs_data = []
    for filename in os.listdir(log_directory):
        if filename.startswith("app-"):  # Assuming filenames start with 'app-'
            event_log_file = os.path.join(log_directory, filename)
            stage_data = extract_stage_info(event_log_file)
            all_runs_data.append({
                "filename": filename,
                "stages": stage_data
            })
    return all_runs_data


# Function to write data to a JSON file
def write_to_json(data, output_file):
    with open(output_file, 'w') as json_file:
        json.dump(data, json_file, indent=4)


# Main function to orchestrate the process
def main():
    log_directory = '/home/dgarg39/spark_profiles_cloudlab_09_10Mar24/spark_logs/event_log/'
    output_file = 'spark_runs_data.json'  # Output JSON file name

    # Process event logs and extract relevant information
    all_runs_data = process_event_logs(log_directory)

    # Write the extracted data to a JSON file
    write_to_json(all_runs_data, output_file)


# Call the main function
if __name__ == "__main__":
    main()
