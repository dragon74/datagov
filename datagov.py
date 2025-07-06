"""
datagov.py

This script divides the work of downloading data from a list of URLs among multiple tasks, as specified by environment variables.

Functionality:
  - Reads environment variables CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT to determine the current task and total number of tasks.
  - Reads URLs from data_urls.txt.
  - Calculates the range of URLs to process for the current task.
  - Streams each assigned URL directly into a Google Cloud Storage bucket, deleting any existing object first.

Environment Variables:
  - CLOUD_RUN_TASK_INDEX: The index of the current task (0-based).
  - CLOUD_RUN_TASK_COUNT: The total number of tasks.
  - GCS_BUCKET_NAME: The name of the GCS bucket to use.

Example usage:
  Set the environment variables in your Google Cloud job configuration.
"""

import os
import math
import urllib.request
import json
import io
import ijson
from google.cloud import storage

# Get environment variables for task division
my_task_num = int(os.environ.get('CLOUD_RUN_TASK_INDEX', '0'))
total_task_num = int(os.environ.get('CLOUD_RUN_TASK_COUNT', '1'))
bucket_name = os.environ['GCS_BUCKET_NAME']

# Read URL objects from the input JSON file
with open('data_urls.json', 'r') as f:
    url_objs = json.load(f)

# Calculate the range of rows assigned to this task
total_rows = len(url_objs)
rows_per_task = math.ceil(total_rows / total_task_num)
start_idx = rows_per_task * my_task_num
end_idx = min(start_idx + rows_per_task, total_rows)

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Stream each assigned URL into GCS, deleting any existing object first

for idx in range(start_idx, end_idx):
    url = url_objs[idx]['url']
    name = url_objs[idx]['name']
    blob_name = f"{name}"
    blob = bucket.blob(blob_name)
    # Delete if exists
    if blob.exists():
        blob.delete()
        print(f"Deleted existing {blob_name} from bucket {bucket_name}")
    try:
        # 1. First request: get total
        with urllib.request.urlopen(url) as fileobj:
            resp_json = json.load(fileobj)
        total = resp_json.get('result', {}).get('total')
        if total is None:
            raise Exception(f"No 'total' field found in response for {url}")

        # 2. Second request: get all records
        # Modify the URL: set limit=total and include_total=false
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        q['limit'] = [str(total)]
        q['include_total'] = ['false']
        # Remove duplicate keys for clean query string
        new_query = urlencode({k: v[0] for k, v in q.items()}, doseq=True)
        new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

        with urllib.request.urlopen(new_url) as fileobj:
            # Use ijson to stream the 'records' field as a string (not as a JSON array)
            parser = ijson.parse(fileobj)
            found = False
            for prefix, event, value in parser:
                if prefix == 'result.records' and event == 'string':
                    found = True
                    # value is the string content of the records field
                    records_bytes = value.encode('utf-8')
                    buffer = io.BytesIO(records_bytes)
                    blob.upload_from_file(buffer, rewind=True)
                    print(f"Downloaded and streamed records string from {new_url} to gs://{bucket_name}/{blob_name}")
                    break
            if not found:
                raise Exception(f"No 'records' string field found in response for {new_url}")
    except Exception as e:
        print(f"Failed to process {url}: {e}")
        exit(1)  # Exit with error if any URL fails

# All URLs processed successfully
print(f"Task {my_task_num} completed processing URLs from index {start_idx} to {end_idx - 1}.")
exit(0)  # Exit successfully after processing all URLs