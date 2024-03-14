import re
import argparse
import csv
from datetime import datetime

# Deep Instinct DPS for NetApp ONTAP Log Analyzer
# created by: Kevin Boerner, March 2024
# kevinb@deepinstinct.com
# Script for analyzing ONTAP verbose logs 
# to calculate avg scantime, througput and processing time
# 
# Version 1.0.0 March 2024
#
# 
# All rights reserved. 
print("""
██████╗ ███████╗███████╗██████╗     ██╗███╗   ██╗███████╗████████╗██╗███╗   ██╗ ██████╗████████╗            
██╔══██╗██╔════╝██╔════╝██╔══██╗    ██║████╗  ██║██╔════╝╚══██╔══╝██║████╗  ██║██╔════╝╚══██╔══╝            
██║  ██║█████╗  █████╗  ██████╔╝    ██║██╔██╗ ██║███████╗   ██║   ██║██╔██╗ ██║██║        ██║               
██║  ██║██╔══╝  ██╔══╝  ██╔═══╝     ██║██║╚██╗██║╚════██║   ██║   ██║██║╚██╗██║██║        ██║               
██████╔╝███████╗███████╗██║         ██║██║ ╚████║███████║   ██║   ██║██║ ╚████║╚██████╗   ██║               
╚═════╝ ╚══════╝╚══════╝╚═╝         ╚═╝╚═╝  ╚═══╝╚══════╝   ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝   ╚═╝               
                                                                                                            
██████╗ ██████╗ ███████╗    ███████╗ ██████╗ ██████╗     ███╗   ██╗███████╗████████╗ █████╗ ██████╗ ██████╗ 
██╔══██╗██╔══██╗██╔════╝    ██╔════╝██╔═══██╗██╔══██╗    ████╗  ██║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗
██║  ██║██████╔╝███████╗    █████╗  ██║   ██║██████╔╝    ██╔██╗ ██║█████╗     ██║   ███████║██████╔╝██████╔╝
██║  ██║██╔═══╝ ╚════██║    ██╔══╝  ██║   ██║██╔══██╗    ██║╚██╗██║██╔══╝     ██║   ██╔══██║██╔═══╝ ██╔═══╝ 
██████╔╝██║     ███████║    ██║     ╚██████╔╝██║  ██║    ██║ ╚████║███████╗   ██║   ██║  ██║██║     ██║     
╚═════╝ ╚═╝     ╚══════╝    ╚═╝      ╚═════╝ ╚═╝  ╚═╝    ╚═╝  ╚═══╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝     ╚═╝     
                                                                                                            
██████╗ ███████╗██████╗ ███████╗ ██████╗ ██████╗ ███╗   ███╗ █████╗ ███╗   ██╗ ██████╗███████╗              
██╔══██╗██╔════╝██╔══██╗██╔════╝██╔═══██╗██╔══██╗████╗ ████║██╔══██╗████╗  ██║██╔════╝██╔════╝              
██████╔╝█████╗  ██████╔╝█████╗  ██║   ██║██████╔╝██╔████╔██║███████║██╔██╗ ██║██║     █████╗                
██╔═══╝ ██╔══╝  ██╔══██╗██╔══╝  ██║   ██║██╔══██╗██║╚██╔╝██║██╔══██║██║╚██╗██║██║     ██╔══╝                
██║     ███████╗██║  ██║██║     ╚██████╔╝██║  ██║██║ ╚═╝ ██║██║  ██║██║ ╚████║╚██████╗███████╗              
╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝╚══════╝              
                                                                                                            
 █████╗ ███╗   ██╗ █████╗ ██╗  ██╗   ██╗███████╗███████╗██████╗     ██╗   ██╗ ██╗                           
██╔══██╗████╗  ██║██╔══██╗██║  ╚██╗ ██╔╝╚══███╔╝██╔════╝██╔══██╗    ██║   ██║███║                           
███████║██╔██╗ ██║███████║██║   ╚████╔╝   ███╔╝ █████╗  ██████╔╝    ██║   ██║╚██║                           
██╔══██║██║╚██╗██║██╔══██║██║    ╚██╔╝   ███╔╝  ██╔══╝  ██╔══██╗    ╚██╗ ██╔╝ ██║                           
██║  ██║██║ ╚████║██║  ██║███████╗██║   ███████╗███████╗██║  ██║     ╚████╔╝  ██║                           
╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═╝   ╚══════╝╚══════╝╚═╝  ╚═╝      ╚═══╝   ╚═╝                           
                                                                                                            
""")

# Set up argument parser
parser = argparse.ArgumentParser(description='Calculate average scan time, total processing time, and optional throughput for files sent to NetApp ONTAP scanner, with optional verbosity.')
parser.add_argument('-p', '--path', type=str, required=True, help='Path to the log file')
parser.add_argument('-l', '--log', type=str, required=False, help='Path to the output CSV file')
parser.add_argument('-size', '--dataset_size', type=float, required=False, help='Dataset size in Megabytes (MB) for throughput calculation')
parser.add_argument('-v', '--verbose', action='store_true', help='Increase output verbosity')

# Parse arguments
args = parser.parse_args()

# Regular expressions to match the lines
send_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z) Sending id (\d+)')
completion_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z) VS_ScanCompletion: request id (\d+)')

# Variables to hold the earliest and latest timestamps
earliest_timestamp = None
latest_timestamp = None

# Dictionary to hold start times
start_times = {}
# List to hold all the durations and IDs for CSV logging
durations_and_ids = []

with open(args.path, 'r') as file:
    for line in file:
        send_match = send_pattern.search(line)
        completion_match = completion_pattern.search(line)

        if send_match:
            timestamp_str, request_id = send_match.groups()
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            if earliest_timestamp is None or timestamp < earliest_timestamp:
                earliest_timestamp = timestamp
            start_times[request_id] = timestamp
        elif completion_match:
            timestamp_str, request_id = completion_match.groups()
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
            if request_id in start_times:
                start_timestamp = start_times[request_id]
                end_timestamp = timestamp
                if args.verbose:
                    print(f"Debug - Start: {start_timestamp}, End: {end_timestamp}, ID: {request_id}")  # Conditional debug print
                duration = end_timestamp - start_timestamp
                duration_ms = duration.total_seconds() * 1000
                durations_and_ids.append((request_id, duration_ms))
                if args.verbose:
                    print(f'Scan time for ID {request_id}: {duration_ms:.2f} ms')  # Conditional print
                del start_times[request_id]

# Calculate the average and total duration
if durations_and_ids and earliest_timestamp and latest_timestamp:
    total_duration_ms = sum(duration for _, duration in durations_and_ids)
    average_duration_ms = total_duration_ms / len(durations_and_ids)
    total_processing_time_s = (latest_timestamp - earliest_timestamp).total_seconds()
    print(f'\nTotal average scan time: {average_duration_ms:.2f} ms')
    print(f'Total processing time for all files: {total_processing_time_s:.2f} s')
    
    # Calculate and print throughput if dataset size is provided
    if args.dataset_size is not None:
        throughput = args.dataset_size / total_processing_time_s  # MB/s
        print(f'Throughput: {throughput:.2f} MB/s')
    
    if args.log:
        with open(args.log, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Request ID', 'Scan Time (ms)'])
            csvwriter.writerows(durations_and_ids)
            csvwriter.writerow(['Average', average_duration_ms])
            csvwriter.writerow(['Total Processing Time (s)', total_processing_time_s])
            if args.dataset_size is not None:
                csvwriter.writerow(['Throughput (MB/s)', throughput])
else:
    print('No scan times found or incomplete data.')
