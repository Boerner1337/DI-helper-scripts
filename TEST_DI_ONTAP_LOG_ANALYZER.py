import re
import argparse
import csv
from datetime import datetime

# Your script's initial comments and metadata

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
parser.add_argument('-id', '--start_id', type=int, required=False, help='Start processing from this ID onwards')

# Parse arguments
args = parser.parse_args()

# Regular expressions to match the lines
send_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z) Sending id (\d+)')
completion_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z) VS_ScanCompletion: request id (\d+)')

# Variables to hold the earliest and latest timestamps, and the highest ID
earliest_timestamp = None
latest_timestamp = None
highest_id = 0

# Initialize a variable to track the actual starting ID used for processing
actual_start_id = None

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
            request_id_int = int(request_id)
            highest_id = max(highest_id, request_id_int)  # Update highest ID

            # Check if this ID meets the starting criteria
            if args.start_id is None or request_id_int >= args.start_id:
                if actual_start_id is None or request_id_int < actual_start_id:
                    actual_start_id = request_id_int  # Update the actual starting ID based on the log

                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                if earliest_timestamp is None or timestamp < earliest_timestamp:
                    earliest_timestamp = timestamp
                start_times[request_id] = timestamp
        elif completion_match:
            timestamp_str, request_id = completion_match.groups()
            request_id_int = int(request_id)
            # No need to update highest_id here, as it's already done in the send_match block

            if request_id in start_times:
                start_timestamp = start_times[request_id]
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                if latest_timestamp is None or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                end_timestamp = timestamp
                if args.verbose:
                    print(f"Debug - Start: {start_timestamp}, End: {end_timestamp}, ID: {request_id}")
                duration = end_timestamp - start_timestamp
                duration_ms = duration.total_seconds() * 1000
                durations_and_ids.append((request_id, duration_ms))
                if args.verbose:
                    print(f'Scan time for ID {request_id}: {duration_ms:.2f} ms')
                del start_times[request_id]

# Final Output - Print Starting ID (if provided) and Highest Request ID
if args.start_id is not None:
    print(f'Starting from ID: {args.start_id}')


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

    # Sort the durations_and_ids list by request ID before writing to the CSV
    if args.log:
        durations_and_ids = sorted(durations_and_ids, key=lambda x: int(x[0]))
        
        with open(args.log, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Request ID', 'Scan Time (ms)'])
            csvwriter.writerows(durations_and_ids)
            csvwriter.writerow(['Average', average_duration_ms])
            csvwriter.writerow(['Total Processing Time (s)', total_processing_time_s])
            if args.dataset_size is not None:
                csvwriter.writerow(['Throughput (MB/s)', throughput])
        print(f'Your log was saved to location: {args.log}')
else:
    print('No scan times found or incomplete data.')

# Feedback about the actual starting ID and highest ID
if args.start_id is not None:
    if actual_start_id is not None:
        print(f"\nSpecified start ID was {args.start_id}. Actual starting ID used for processing: {actual_start_id}.")
    else:
        print(f"\nSpecified start ID was {args.start_id}, but no IDs equal to or greater than this were found in the log.")

# Print the highest request ID message only once, here at the end
print(f"Highest request ID in the log: {highest_id}")
