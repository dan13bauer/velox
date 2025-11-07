#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Extract individual state transition metrics from 4-server log files.
Creates a CSV with all individual transitions from server and client logs.
"""

import csv
from pathlib import Path
from collections import defaultdict

# Configuration
LOG_DIR = Path("/gpfs/zc2/u/dnb/velox/logs/4_servers")
OUTPUT_FILE = Path(
    "/gpfs/zc2/u/dnb/velox/performance_report_4_servers/4_servers_combined_transitions.csv"
)
BUFFER_SIZES = [512, 1024, 2048, 4096]

# State transitions to extract
SERVER_TRANSITION = "WaitingForSendComplete -> ReadyToTransfer"
CLIENT_TRANSITION = "WaitingForData -> ReadyToReceive"


def extract_transitions_from_log(log_file, transition_name):
    """
    Extract individual transition data from a log file.

    Returns:
        List of tuples: (index, duration_us, bytes, throughput_MB_s)
    """
    transitions = []

    with open(log_file, "r") as f:
        lines = f.readlines()

    # Find the transition section
    in_target_section = False
    in_individual_section = False

    for i, line in enumerate(lines):
        # Look for the specific transition header
        if transition_name in line:
            in_target_section = True
            continue

        # Look for "Individual transitions:" within the target section
        if in_target_section and "Individual transitions:" in line:
            in_individual_section = True
            # Skip the header line "Index, Duration µs, Bytes, Throughput MB/s"
            continue

        # Start extracting data after finding the table header
        if in_individual_section and "Index, Duration" in line:
            # Now read the data lines
            j = i + 1
            while j < len(lines):
                data_line = lines[j].strip()

                # Stop if we hit an empty line or a line that starts with a letter (next section)
                if not data_line or (data_line and data_line[0].isalpha()):
                    break

                # Parse the data line: "0, 1138756, 536777088, 449.535"
                # Handle lines with or without throughput
                parts = [p.strip() for p in data_line.split(",")]
                if len(parts) >= 3:
                    try:
                        index = int(parts[0])
                        duration_us = int(parts[1])
                        bytes_val = int(parts[2])
                        throughput_MB_s = float(parts[3]) if len(parts) >= 4 else 0.0

                        transitions.append(
                            (index, duration_us, bytes_val, throughput_MB_s)
                        )
                    except (ValueError, IndexError):
                        # Skip malformed lines
                        pass

                j += 1

            # We found and processed the section, no need to continue
            break

        # Reset if we've moved to a different transition
        if (
            in_target_section
            and line.strip()
            and line[0].isalpha()
            and "->" in line
            and transition_name not in line
        ):
            in_target_section = False
            in_individual_section = False

    return transitions


def main():
    """Main function to extract transitions and create CSV."""

    all_data = []
    stats = defaultdict(lambda: defaultdict(int))

    # Process server logs (server_1 through server_4)
    for server_num in range(1, 5):
        server_name = f"server_{server_num}"

        for buf_size in BUFFER_SIZES:
            log_file = LOG_DIR / f"{server_name}_bufSize_{buf_size}.log"

            if not log_file.exists():
                print(f"Warning: {log_file} not found, skipping...")
                continue

            print(f"Processing {log_file.name}...")

            transitions = extract_transitions_from_log(log_file, SERVER_TRANSITION)

            for index, duration_us, bytes_val, throughput in transitions:
                all_data.append(
                    {
                        "source": server_name,
                        "bufSize": buf_size,
                        "index": index,
                        "duration_us": duration_us,
                        "bytes": bytes_val,
                        "throughput_MB_s": throughput,
                    }
                )
                stats[server_name][buf_size] += 1
                stats[server_name]["total"] += 1

    # Process client log
    for buf_size in BUFFER_SIZES:
        log_file = LOG_DIR / f"client_bufSize_{buf_size}.log"

        if not log_file.exists():
            print(f"Warning: {log_file} not found, skipping...")
            continue

        print(f"Processing {log_file.name}...")

        transitions = extract_transitions_from_log(log_file, CLIENT_TRANSITION)

        for index, duration_us, bytes_val, throughput in transitions:
            all_data.append(
                {
                    "source": "client",
                    "bufSize": buf_size,
                    "index": index,
                    "duration_us": duration_us,
                    "bytes": bytes_val,
                    "throughput_MB_s": throughput,
                }
            )
            stats["client"][buf_size] += 1
            stats["client"]["total"] += 1

    # Write CSV file
    print(f"\nWriting results to {OUTPUT_FILE}...")

    with open(OUTPUT_FILE, "w", newline="") as csvfile:
        fieldnames = [
            "source",
            "bufSize",
            "index",
            "duration_us",
            "bytes",
            "throughput_MB_s",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in all_data:
            writer.writerow(row)

    print(f"\nSuccessfully wrote {len(all_data)} transitions to {OUTPUT_FILE}")

    # Print statistics
    print("\n" + "=" * 70)
    print("EXTRACTION STATISTICS")
    print("=" * 70)

    # Server statistics
    for server_num in range(1, 5):
        server_name = f"server_{server_num}"
        if server_name in stats:
            print(f"\n{server_name.upper()}:")
            for buf_size in BUFFER_SIZES:
                count = stats[server_name].get(buf_size, 0)
                print(f"  Buffer {buf_size:4d} MB: {count:3d} transitions")
            print(f"  Total:          {stats[server_name]['total']:3d} transitions")

    # Client statistics
    if "client" in stats:
        print("\nCLIENT:")
        for buf_size in BUFFER_SIZES:
            count = stats["client"].get(buf_size, 0)
            print(f"  Buffer {buf_size:4d} MB: {count:3d} transitions")
        print(f"  Total:          {stats['client']['total']:3d} transitions")

    # Grand total
    grand_total = sum(s["total"] for s in stats.values())
    print(f"\n{'=' * 70}")
    print(f"GRAND TOTAL: {grand_total} transitions")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
