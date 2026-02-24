"""
json_reader.py — JSON Deserialization Module
=============================================
This module handles READING the JSON file and converting it
into a flat list of tag data (like a C# DataTable).

What is Deserialization?
  - JSON file is just TEXT on disk
  - json.load() converts that TEXT → Python dictionary/list objects
  - This process is called "deserialization" (text → object)
  - In C# you'd use JsonConvert.DeserializeObject<T>()
"""

import json
import os
import sys


def read_json_file(file_path):
    """
    Reads and deserializes a JSON file into a Python dictionary.

    Args:
        file_path (str): Full path to the JSON file

    Returns:
        dict: The deserialized JSON data as a Python dictionary

    How it works:
        1. Checks if the file exists
        2. Opens the file with utf-8-sig encoding (handles BOM marker)
        3. json.load() reads the entire file and converts JSON text
           into Python objects (dicts, lists, strings, numbers)
    """
    if not os.path.exists(file_path):
        print(f"❌ ERROR: JSON file not found at: {file_path}")
        sys.exit(1)

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📖 Reading JSON file ({file_size_mb:.1f} MB)...")

    # encoding="utf-8-sig" strips the BOM (Byte Order Mark) if present
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    print("✅ JSON deserialized successfully!")
    return data


def extract_tags(data):
    """
    Extracts all tag data from the deserialized JSON into a flat list.
    This is like building a C# DataTable with rows.

    Args:
        data (dict): The deserialized JSON data

    Returns:
        list: A list of tuples, each tuple = one row:
              (channel_name, device_name, device_id_string, tag_name, address, data_type, scan_rate)

    JSON Structure:
        project
         └── channels[]                        ← Loop 1: each channel
              ├── common.ALLTYPES_NAME          ← channel_name
              └── devices[]                     ← Loop 2: each device
                   ├── common.ALLTYPES_NAME     ← device_name
                   └── tags[]                   ← Loop 3: each tag
                        ├── common.ALLTYPES_NAME              ← tag_name
                        ├── servermain.TAG_ADDRESS             ← address
                        ├── servermain.TAG_DATA_TYPE           ← data_type
                        └── servermain.TAG_SCAN_RATE_MILLISECONDS ← scan_rate
    """
    rows = []

    channels = data.get("project", {}).get("channels", [])
    print(f"📊 Found {len(channels)} channels in JSON")

    for channel in channels:
        channel_name = channel.get("common.ALLTYPES_NAME", "")

        for device in channel.get("devices", []):
            device_name = device.get("common.ALLTYPES_NAME", "")
            device_id_string = device.get("servermain.DEVICE_ID_STRING", "")

            for tag in device.get("tags", []):
                rows.append((
                    channel_name,
                    device_name,
                    device_id_string,
                    tag.get("common.ALLTYPES_NAME"),
                    tag.get("servermain.TAG_ADDRESS"),
                    tag.get("servermain.TAG_DATA_TYPE"),
                    tag.get("servermain.TAG_SCAN_RATE_MILLISECONDS")
                ))

    print(f"✅ Extracted {len(rows)} tag rows from JSON")
    return rows
