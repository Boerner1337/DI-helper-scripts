import argparse
import re
import os
from datetime import datetime
import csv

# Deep Instinct DPS for NetApp ONTAP Log Analyzer
# created by: Kevin Boerner, March 2024
# kevinb@deepinstinct.com
# Script for analyzing ONTAP verbose logs 
# to calculate avg scantime, throughput and processing time
# from: https://github.com/Boerner1337/DI-helper-scripts
# Version 4.1.0 August 2024
# 
# 
# All rights reserved. 

def display_ascii_art():
    ascii_art = """
    ____                     ____           __  _            __
   / __ \\___  ___  ____     /  _/___  _____/ /_(_)___  _____/ /_
  / / / / _ \\/ _ \\/ __ \\    / // __ \\/ ___/ __/ / __ \\/ ___/ __/
 / /_/ /  __/  __/ /_/ /  _/ // / / (__  ) /_/ / / / / /__/ /_
/_____/\___/\___/ .___/  /___/_/ /_/____/\\__/_/_/ /_/\___/\\__/
   ____  _   __/_/______    ____     __                   ___                __
  / __ \\/ | / /_  __/   |  / __ \\   / /   ____  ____ _   /   |  ____  ____ _/ /_  ______  ___  _____
 / / / /  |/ / / / / /| | / /_/ /  / /   / __ \\/ __ `/  / /| | / __ \\/ __ `/ / / / /_  / / _ \\/ ___/
/ /_/ / /|  / / / / ___ |/ ____/  / /___/ /_/ / /_/ /  / ___ |/ / / / /_/ / / /_/ / / /_/  __/ /    
\\____/_/ |_/_/ /_/_/  |_/_/      /_____/\\____/\\__, /  /_/  |_/_/ /_/\\__,_/_/\\__, / /___/\\___/_/     
                                             /____/                        /____/                 
    """
    print(ascii_art)

def parse_ontap_log_with_correct_throughput(logfile, verbose=False, start_id=None):
    sending_re = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z) Sending id (\d+).*\\\\\?\\UNC(.*)")
    completion_re = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z) VS_ScanCompletion: request id (\d+).*completed.*")
    
    events = {}
    
    for line in logfile:
        sending_match = sending_re.match(line)
        completion_match = completion_re.match(line)
        
        if sending_match:
            timestamp, id_str, path = sending_match.groups()
            id = int(id_str)
            events[id] = {"sending_time": datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"), "path": path}
        elif completion_match:
            timestamp, id_str = completion_match.groups()
            id = int(id_str)
            if id in events:
                events[id]["completion_time"] = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

    if start_id is not None:
        events = {id: val for id, val in events.items() if id >= start_id}
    
    return events

def calculate_statistics(events, verbose=False):
    total_scan_time = 0
    scan_times = []
    for id, event in events.items():
        if "completion_time" in event:
            scan_time = (event["completion_time"] - event["sending_time"]).total_seconds() * 1000
            total_scan_time += scan_time
            scan_times.append((event["path"], scan_time))
    
    avg_scan_time = total_scan_time / len(scan_times) if scan_times else 0
    total_processing_time = (max(event["completion_time"] for event in events.values() if "completion_time" in event) - 
                             min(event["sending_time"] for event in events.values())).total_seconds() if events else 0

    if verbose:
        for path, scan_time in scan_times:
            print(f"{path}: {round(scan_time, 2)} ms")

    return avg_scan_time, total_processing_time, scan_times

def process_directory(directory, verbose=False, start_id=None):
    total_events = {}

    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path):
            print(f"Processing {file_path}")
            with open(file_path, 'r', encoding='utf-8', errors='replace') as logfile:
                events = parse_ontap_log_with_correct_throughput(logfile, verbose, start_id)
                total_events.update(events)

    return total_events

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse ONTAP Trace logs and analyze AV scanner performance.")
    parser.add_argument("-p", "--path", help="Path to the log file")
    parser.add_argument("-d", "--directory", help="Directory containing log files")
    parser.add_argument("-o", "--output", help="Path to the output CSV file")
    parser.add_argument("-size", "--data-size", type=int, help="Size of the dataset in MB for throughput calculation")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-id", "--start-id", type=int, help="Start calculations from this ID")
    args = parser.parse_args()

    display_ascii_art()

    events = {}

    if args.directory:
        events = process_directory(args.directory, args.verbose, args.start_id)
    elif args.path:
        with open(args.path, 'r', encoding='utf-8', errors='replace') as logfile:
            events = parse_ontap_log_with_correct_throughput(logfile, args.verbose, args.start_id)
    else:
        print("Either --path or --directory must be provided.")
        exit(1)

    if events:
        all_ids = sorted(events.keys())
        total_ids = len(all_ids)
        lowest_id, highest_id = min(all_ids), max(all_ids)

        if args.directory:
            print(f"Total IDs processed: {total_ids}")
        else:
            print(f"Total IDs: {total_ids}, Lowest ID: {lowest_id}, Highest ID: {highest_id}")

            if args.start_id is not None:
                nearest_id = next((id for id in all_ids if id >= args.start_id), None)
                if nearest_id is not None:
                    print(f"Using ID: {nearest_id} (nearest to requested {args.start_id})")
                else:
                    print(f"No ID found equal or higher than {args.start_id}. Exiting.")
                    exit(1)
            else:
                print("Calculating from the lowest ID found.")
        
        avg_scan_time, total_processing_time, scan_times = calculate_statistics(events, args.verbose)

        if args.directory:
            print(f"Average scan time for all files processed: {round(avg_scan_time, 2)} ms")
            print(f"Total processing time for all files processed: {round(total_processing_time, 2)} seconds")
        else:
            print(f"Average Scan Time: {round(avg_scan_time, 2)} ms")
            print(f"Total Processing Time: {round(total_processing_time, 2)} seconds")

        if args.data_size:
            throughput = float(args.data_size) / total_processing_time  # Corrected throughput calculation
            print(f"Throughput: {round(throughput, 2)} MB/s")

        if args.output:
            with open(args.output, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Path", "Scan Time (ms)"])
                for path, scan_time in scan_times:
                    writer.writerow([path, round(scan_time, 2)])
    else:
        print("No valid events found.")
