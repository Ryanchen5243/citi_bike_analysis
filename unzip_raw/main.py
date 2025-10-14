import boto3
import os
import zipfile
import io
import tempfile

def flatten_raw_data():
    nyc_files_1 = ["2013-citibike-tripdata.zip",
        "2014-citibike-tripdata.zip",
        "2015-citibike-tripdata.zip",
        "2016-citibike-tripdata.zip",
        "2017-citibike-tripdata.zip",
        "2018-citibike-tripdata.zip",
        "2019-citibike-tripdata.zip",
        "2020-citibike-tripdata.zip",
        "2021-citibike-tripdata.zip",
        "2022-citibike-tripdata.zip",
        "2023-citibike-tripdata.zip"]
    nyc_files_2 = ["202401-citibike-tripdata.zip",
        "202402-citibike-tripdata.zip",
        "202403-citibike-tripdata.zip",
        "202404-citibike-tripdata.zip",
        "202405-citibike-tripdata.zip",
        "202406-citibike-tripdata.zip",
        "202407-citibike-tripdata.zip",
        "202408-citibike-tripdata.zip",
        "202409-citibike-tripdata.zip",
        "202410-citibike-tripdata.zip",
        "202411-citibike-tripdata.zip",
        "202412-citibike-tripdata.zip"]
    nyc_files_3 = ["202501-citibike-tripdata.zip",
        "202502-citibike-tripdata.zip",
        "202503-citibike-tripdata.zip",
        "202504-citibike-tripdata.zip",
        "202505-citibike-tripdata.zip",
        "202506-citibike-tripdata.zip",
        "202507-citibike-tripdata.zip",
        "202508-citibike-tripdata.zip",
        "202509-citibike-tripdata.zip"]    
    jc_files = ["JC-201509-citibike-tripdata.csv.zip",
        "JC-201510-citibike-tripdata.csv.zip",
        "JC-201511-citibike-tripdata.csv.zip",
        "JC-201512-citibike-tripdata.csv.zip",
        "JC-201601-citibike-tripdata.csv.zip",
        "JC-201602-citibike-tripdata.csv.zip",
        "JC-201603-citibike-tripdata.csv.zip",
        "JC-201604-citibike-tripdata.csv.zip",
        "JC-201605-citibike-tripdata.csv.zip",
        "JC-201606-citibike-tripdata.csv.zip",
        "JC-201607-citibike-tripdata.csv.zip",
        "JC-201608-citibike-tripdata.csv.zip",
        "JC-201609-citibike-tripdata.csv.zip",
        "JC-201610-citibike-tripdata.csv.zip",
        "JC-201611-citibike-tripdata.csv.zip",
        "JC-201612-citibike-tripdata.csv.zip",
        "JC-201701-citibike-tripdata.csv.zip",
        "JC-201702-citibike-tripdata.csv.zip",
        "JC-201703-citibike-tripdata.csv.zip",
        "JC-201704-citibike-tripdata.csv.zip",
        "JC-201705-citibike-tripdata.csv.zip",
        "JC-201706-citibike-tripdata.csv.zip",
        "JC-201707-citibike-tripdata.csv.zip",
        "JC-201709-citibike-tripdata.csv.zip",
        "JC-201710-citibike-tripdata.csv.zip",
        "JC-201711-citibike-tripdata.csv.zip",
        "JC-201712-citibike-tripdata.csv.zip",
        "JC-201801-citibike-tripdata.csv.zip",
        "JC-201802-citibike-tripdata.csv.zip",
        "JC-201803-citibike-tripdata.csv.zip",
        "JC-201804-citibike-tripdata.csv.zip",
        "JC-201805-citibike-tripdata.csv.zip",
        "JC-201806-citibike-tripdata.csv.zip",
        "JC-201807-citibike-tripdata.csv.zip",
        "JC-201808-citibike-tripdata.csv.zip",
        "JC-201809-citibike-tripdata.csv.zip",
        "JC-201810-citibike-tripdata.csv.zip",
        "JC-201811-citibike-tripdata.csv.zip",
        "JC-201812-citibike-tripdata.csv.zip",
        "JC-201901-citibike-tripdata.csv.zip",
        "JC-201902-citibike-tripdata.csv.zip",
        "JC-201903-citibike-tripdata.csv.zip",
        "JC-201904-citibike-tripdata.csv.zip",
        "JC-201905-citibike-tripdata.csv.zip",
        "JC-201906-citibike-tripdata.csv.zip",
        "JC-201907-citibike-tripdata.csv.zip",
        "JC-201908-citibike-tripdata.csv.zip",
        "JC-201909-citibike-tripdata.csv.zip",
        "JC-201910-citibike-tripdata.csv.zip",
        "JC-201911-citibike-tripdata.csv.zip",
        "JC-201912-citibike-tripdata.csv.zip",
        "JC-202001-citibike-tripdata.csv.zip",
        "JC-202002-citibike-tripdata.csv.zip",
        "JC-202003-citibike-tripdata.csv.zip",
        "JC-202004-citibike-tripdata.csv.zip",
        "JC-202005-citibike-tripdata.csv.zip",
        "JC-202006-citibike-tripdata.csv.zip",
        "JC-202007-citibike-tripdata.csv.zip",
        "JC-202008-citibike-tripdata.csv.zip",
        "JC-202009-citibike-tripdata.csv.zip",
        "JC-202010-citibike-tripdata.csv.zip",
        "JC-202011-citibike-tripdata.csv.zip",
        "JC-202012-citibike-tripdata.csv.zip",
        "JC-202101-citibike-tripdata.csv.zip",
        "JC-202102-citibike-tripdata.csv.zip",
        "JC-202103-citibike-tripdata.csv.zip",
        "JC-202104-citibike-tripdata.csv.zip",
        "JC-202105-citibike-tripdata.csv.zip",
        "JC-202106-citibike-tripdata.csv.zip",
        "JC-202107-citibike-tripdata.csv.zip",
        "JC-202108-citibike-tripdata.csv.zip",
        "JC-202109-citibike-tripdata.csv.zip",
        "JC-202110-citibike-tripdata.csv.zip",
        "JC-202111-citibike-tripdata.csv.zip",
        "JC-202112-citibike-tripdata.csv.zip",
        "JC-202201-citibike-tripdata.csv.zip",
        "JC-202202-citibike-tripdata.csv.zip",
        "JC-202203-citibike-tripdata.csv.zip",
        "JC-202204-citibike-tripdata.csv.zip",
        "JC-202205-citibike-tripdata.csv.zip",
        "JC-202206-citibike-tripdata.csv.zip",
        "JC-202207-citbike-tripdata.csv.zip",
        "JC-202208-citibike-tripdata.csv.zip",
        "JC-202209-citibike-tripdata.csv.zip",
        "JC-202210-citibike-tripdata.csv.zip",
        "JC-202211-citibike-tripdata.csv.zip",
        "JC-202212-citibike-tripdata.csv.zip",
        "JC-202301-citibike-tripdata.csv.zip",
        "JC-202302-citibike-tripdata.csv.zip",
        "JC-202303-citibike-tripdata.csv.zip",
        "JC-202304-citibike-tripdata.csv.zip",
        "JC-202305-citibike-tripdata.csv.zip",
        "JC-202306-citibike-tripdata.csv.zip",
        "JC-202307-citibike-tripdata.csv.zip",
        "JC-202308-citibike-tripdata.csv.zip",
        "JC-202309-citibike-tripdata.csv.zip",
        "JC-202310-citibike-tripdata.csv.zip",
        "JC-202311-citibike-tripdata.csv.zip",
        "JC-202312-citibike-tripdata.csv.zip",
        "JC-202401-citibike-tripdata.csv.zip",
        "JC-202402-citibike-tripdata.csv.zip",
        "JC-202403-citibike-tripdata.csv.zip",
        "JC-202404-citibike-tripdata.csv.zip",
        "JC-202405-citibike-tripdata.csv.zip",
        "JC-202406-citibike-tripdata.csv.zip",
        "JC-202407-citibike-tripdata.csv.zip",
        "JC-202408-citibike-tripdata.csv.zip",
        "JC-202409-citibike-tripdata.csv.zip",
        "JC-202410-citibike-tripdata.csv.zip",
        "JC-202411-citibike-tripdata.csv.zip",
        "JC-202412-citibike-tripdata.csv.zip",
        "JC-202501-citibike-tripdata.csv.zip",
        "JC-202502-citibike-tripdata.csv.zip",
        "JC-202503-citibike-tripdata.csv.zip",
        "JC-202504-citibike-tripdata.csv.zip",
        "JC-202505-citibike-tripdata.csv.zip",
        "JC-202506-citibike-tripdata.csv.zip",
        "JC-202507-citibike-tripdata.csv.zip",
        "JC-202508-citibike-tripdata.csv.zip",
        "JC-202509-citibike-tripdata.csv.zip"]
    unzip_and_write(nyc_files_1,"nyc")
    unzip_and_write(nyc_files_2,"nyc")
    unzip_and_write(nyc_files_3,"nyc")
    unzip_and_write(jc_files,"jc")

def unzip_and_write(zip_obj_list, nyc_or_jc):
    s3_client = boto3.client("s3")
    def extract_zip(file_bytes, extract_to, zip_prefix):
        """Recursively extract ZIPs and return list of (local_path, original_name) tuples."""
        csv_files = []
        with zipfile.ZipFile(io.BytesIO(file_bytes), 'r') as zf:
            for f in zf.namelist():
                if f.startswith('__MACOSX') or '/._' in f or f.endswith('.DS_Store'):
                    print("skipping file -> ",f)
                    continue
                data = zf.read(f)
                if f.lower().endswith(".csv"):
                    base_name = os.path.splitext(os.path.basename(f))[0]
                    dst_name = f"{base_name}_{zip_prefix}.csv"
                    dst = os.path.join(extract_to, dst_name)
                    with open(dst, 'wb') as out_f:
                        out_f.write(data)
                    csv_files.append(dst)
                elif f.lower().endswith(".zip"):
                    print(f"now processing zip file {f} of length {len(data)}")
                    inner_extract_to = tempfile.mkdtemp(dir=extract_to)
                    inner_prefix = f"{zip_prefix}_{os.path.splitext(os.path.basename(f))[0]}"
                    csv_files.extend(extract_zip(data, inner_extract_to, inner_prefix))
        return csv_files

    for s3_file in zip_obj_list:
        try:
            print(f"\n=== Now fetching -> {s3_file} ===")
            obj = s3_client.get_object(Bucket="citibike-nycdata", Key=f"raw/{s3_file}")
            stream = obj['Body']
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_folder = os.path.join(temp_dir, "flattened")
                os.makedirs(temp_folder, exist_ok=True)
                # Extract all CSVs (including nested ZIPs) with unique names
                zip_prefix = os.path.splitext(s3_file)[0]  # e.g., 202401-citibike-tripdata
                file_bytes = stream.read()
                all_csvs = extract_zip(file_bytes, temp_folder, zip_prefix)
                # Upload CSVs to S3
                for local_file_path in all_csvs:
                    sub_dir = "nyc_files" if nyc_or_jc == "nyc" else "jc_files"
                    year = s3_file[:4] if nyc_or_jc == "nyc" else s3_file[3:7]
                    s3_key = f"flatten_raw/{sub_dir}/{year}/{os.path.basename(local_file_path)}"
                    print(f"Uploading {local_file_path} -> s3://citibike-nycdata/{s3_key}")
                    s3_client.upload_file(local_file_path, "citibike-nycdata", s3_key)
            stream.close()
        except Exception as e:
            print(f"ERROR processing {s3_file}: {e}")

if __name__ == "__main__":
    flatten_raw_data()