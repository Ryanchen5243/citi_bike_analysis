# aws s3 ls s3://tripdata | awk '{print $4}' > tripdata_files.txt
s3_filenames="tripdata_files.txt"
if [[ ! -f "$s3_filenames" ]]; then
    echo "Error: File '$s3_filenames' not found"
    exit 1
fi

while IFS= read -r line; do
    src="s3://tripdata/$line"
    dest="s3://citibike-nycdata/raw/$line"

    if [[ "$line" == *.zip ]]; then
        aws s3 cp "$src" "$dest" \
            --metadata-directive REPLACE \
            --content-type application/zip \
            --only-show-errors
    else
        aws s3 cp "$src" "$dest" \
            --metadata-directive REPLACE \
            --only-show-errors
    fi

    echo "✅ Copied $line"
done < "$s3_filenames"
