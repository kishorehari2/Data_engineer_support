import json
import sys
import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError
from urllib.parse import urlparse, parse_qs

def extract_paths_from_raw(raw_content):
    """
    Handle raw field which could be JSON or URL with query params.
    Returns list of (bucket, key) tuples.
    """
    paths = []
    try:
        # Case 1: raw is JSON
        body_json = json.loads(raw_content)
        for key, val in body_json.items():
            if key.endswith(("_key", "_file", "_path")) and val:
                bucket = body_json.get(
                    key.replace("_key", "_bucket"),
                    body_json.get(key.replace("_file", "_bucket"),
                    body_json.get(key.replace("_path", "_bucket"), ""))
                )
                if bucket:
                    paths.append((bucket, val))
    except json.JSONDecodeError:
        # Case 2: raw is URL
        parsed = urlparse(raw_content)
        query_params = parse_qs(parsed.query)
        bucket = None
        for key, values in query_params.items():
            if "bucket" in key:
                bucket = values[0]
            if key.endswith(("file", "path")) and values:
                if bucket:
                    paths.append((bucket, values[0]))
    return paths

def check_service(json_path, service_name):
    # Load the JSON collection
    with open(json_path, "r") as f:
        data = json.load(f)

    # Find the service block
    service_item = next((item for item in data["item"] if item["name"] == service_name), None)
    if not service_item:
        print(f"Service '{service_name}' not found in JSON")
        return

    paths = []

    # Handle body.raw
    if "body" in service_item["request"] and "raw" in service_item["request"]["body"]:
        raw_content = service_item["request"]["body"]["raw"]
        paths.extend(extract_paths_from_raw(raw_content))

    # Handle url.raw (in case query params are there)
    if "url" in service_item["request"] and "raw" in service_item["request"]["url"]:
        raw_content = service_item["request"]["url"]["raw"]
        paths.extend(extract_paths_from_raw(raw_content))

    s3 = boto3.client("s3")

    file_access_failed = []
    room_category_failed = []
    empty_file_failed = []
    files_checked = 0  # track only successfully read files

    for bucket, key in paths:
        s3_path = f"s3://{bucket}/{key}"
        try:
            # Accessibility check
            s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            reason = "unable to access the file"
            file_access_failed.append((s3_path, reason))
            continue  # Skip further checks if not accessible

        try:
            if key.endswith(".parquet"):
                obj = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_parquet(BytesIO(obj["Body"].read()))
                files_checked += 1

                if df.empty:
                    empty_file_failed.append(s3_path)

                if "room_category" not in df.columns:
                    room_category_failed.append(s3_path)

            elif key.endswith(".json"):
                obj = s3.get_object(Bucket=bucket, Key=key)
                content = json.loads(obj["Body"].read())
                files_checked += 1

                if isinstance(content, dict):
                    if "room_category" not in content:
                        room_category_failed.append(s3_path)

        except Exception as e:
            file_access_failed.append((s3_path, str(e)))

    # Print results
    if file_access_failed:
        print("file_accessible : Failed")
        for p, reason in file_access_failed:
            print(f"  {p} -> {reason}")
    else:
        print("file_accessible : Passed")

    # If no files could be checked -> mark checks as Failed
    if files_checked == 0:
        print("room_category_check : Failed (no accessible files)")
        print("empty_file : Failed (no accessible files)")
        return

    if room_category_failed:
        print("room_category_check : Failed")
        for p in room_category_failed:
            print(f"  {p}")
    else:
        print("room_category_check : Passed")

    if empty_file_failed:
        print("empty_file : Failed")
        for p in empty_file_failed:
            print(f"  {p}")
    else:
        print("empty_file : Passed")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python check_quality.py <json_file_path> <service_name>")
        sys.exit(1)

    check_service(sys.argv[1], sys.argv[2])
