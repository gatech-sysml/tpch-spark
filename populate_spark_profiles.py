import json
import os
import statistics
from collections import defaultdict

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EventLogReader") \
    .getOrCreate()


# Function to extract statistics from event log files
def extract_statistics(event_logs):
    # Initialize data structures to collect statistics
    app_statistics = defaultdict(dict)
    all_stage_dependencies = defaultdict(set)

    # Process event log files
    for event_log_file in event_logs:
        with open(event_log_file, 'r') as f:
            events = f.readlines()

        stage_runtimes = defaultdict(int)
        task_counts = defaultdict(int)
        task_runtimes = defaultdict(list)
        stage_dependencies = defaultdict(list)

        # Iterate over events to collect stage and task information
        for line in events:
            event_data = json.loads(line.strip())
            event_type = event_data.get("Event", None)
            # print("line: ", line)

            if event_type == "SparkListenerStageCompleted":
                stage_id = event_data.get("Stage Info", {}).get("Stage ID", None)
                completion_time = event_data.get("Stage Info", {}).get("Completion Time", None)
                # parent_ids = event_data.get("Stage Info", {}).get("Parent IDs", [])

                if stage_id is not None and completion_time is not None:
                    # Collect stage runtime
                    stage_runtime = completion_time - event_data.get(
                        "Stage Info", {}).get("Submission Time", 0)
                    stage_runtimes[stage_id] += stage_runtime

                    # Collect task information
                    for task_info in event_data.get("Task Info", []):
                        task_runtimes[stage_id].append(
                            task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0))
                        task_counts[stage_id] += 1

                    # # Collect stage dependencies
                    # if stage_id is not None and parent_ids:
                    #     stage_dependencies[stage_id].update(parent_ids)
                    #     # Recursively add the parents of parent IDs
                    #     for parent_id in parent_ids:
                    #         stage_dependencies[stage_id].update(
                    #             traverse_parents(parent_id, stage_dependencies))

            elif event_type == "SparkListenerTaskEnd":
                stage_id = event_data.get("Stage ID", None)
                if stage_id is not None:
                    task_runtimes[stage_id].append(event_data.get("Task Info", {}).get("Finish Time", 0) -
                                                    event_data.get("Task Info", {}).get("Launch Time", 0))
                    task_counts[stage_id] += 1

            elif event_type == "SparkListenerJobStart":
                stages = event_data.get("Stage Infos", [])
                job_id = event_data.get("Job ID", None)

                # Extract stage information and dependencies
                for stage in stages:
                    stage_id = stage.get("Stage ID", None)
                    parent_ids = stage.get("Parent IDs", [])

                    if stage_id is not None and parent_ids:
                        stage_dependencies[stage_id].extend(parent_ids)
                        # Recursively add the parents of parent IDs
                        for parent_id in parent_ids:
                            stage_dependencies[stage_id].extend(
                                traverse_parents(parent_id, stage_dependencies))
            
            elif event_type == "SparkListenerEnvironmentUpdate":
                environment_details = event_data.get("Spark Properties", {})
                # app_id = environment_details.get("spark.app.id")
                app_name = environment_details.get("spark.app.name")

        # Calculate median task runtimes
        median_task_runtimes = {stage_id: statistics.median(runtimes) if len(runtimes) > 0 else 0
                                for stage_id, runtimes in task_runtimes.items()}

        # Add statistics to app_statistics dictionary with app name and id as keys
        app_statistics[f"{app_name}"]["StageRuntimes"] = stage_runtimes
        app_statistics[f"{app_name}"]["TaskCounts"] = task_counts
        app_statistics[f"{app_name}"]["MedianTaskRuntimes"] = median_task_runtimes
        app_statistics[f"{app_name}"]["StageDependencies"] = stage_dependencies

        print("Processed for application: ", event_log_file)
        print("Populated statistics: ", app_statistics)

        # break

    return app_statistics


# Recursive function to traverse parents of parent IDs
def traverse_parents(parent_id, stage_dependencies):
    parents = stage_dependencies.get(parent_id, [])
    if not parents:
        return []
    else:
        all_parents = []
        for parent in parents:
            all_parents.extend(traverse_parents(parent, stage_dependencies))
        return all_parents


# Function to get list of event log files from a directory
def get_event_log_files(directory):
    event_log_files = []
    for file_name in os.listdir(directory):
        if file_name.startswith("app-"):
            event_log_files.append(os.path.join(directory, file_name))
    return event_log_files


# Directory containing event log files
log_directory = '/home/dgarg39/spark_profiles_cloudlab_09_10Mar24/spark_logs/event_log'

# Get list of event log files
log_files = get_event_log_files(log_directory)

# Load event log files
# Replace "/path/to/spark/logs" with the actual path to your Spark logs directory
# log_files = [
#     '/home/dgarg39/spark_profiles_cloudlab_09_10Mar24/spark_logs/event_log/app-20240309173424-0496']

# Extract statistics from event log files
got_app_statistics = extract_statistics(log_files)

# Write statistics to JSON file
with open("spark_statistics.json", "w") as f:
    json.dump(got_app_statistics, f, indent=4)

print("Statistics written to spark_statistics.json")
