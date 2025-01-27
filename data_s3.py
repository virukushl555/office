
import csv
import boto3
import psycopg2
import logging
import json
from datetime import datetime

# Environment variables for database connection
db_host = os.environ['DB_HOST']
db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
--port
query = "SELECT * FROM your_table WHERE status = 'timeout' AND date_column = CURRENT_DATE"

--logger
--logger

def lambda_handler(event, context):

    # S3 bucket and file settings
    s3_bucket = os.environ['S3_BUCKET']
    s3_key = f"timeout_records_{datetime.now().strftime('%Y-%m-%d')}.csv"
    csv_file_path = f"/tmp/{s3_key}"  # Temp directory in Lambda

    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = connection.cursor()
        cursor.execute(query)

        # Fetch query results
        rows = cursor.fetchall()

        # Write results to a CSV file
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write headers (column names)
            csvwriter.writerow([desc[0] for desc in cursor.description])
            # Write rows (data)
            csvwriter.writerows(rows)

        # Upload the CSV file to S3
        s3 = boto3.client('s3')
        s3.upload_file(csv_file_path, s3_bucket, s3_key)

        # Log success and return response
        print(f"File {s3_key} successfully uploaded to bucket {s3_bucket}.")
        return {
            "statusCode": 200,
            "body": f"File {s3_key} successfully uploaded to bucket {s3_bucket}."
        }

    except Exception as e:
        # Log any errors and return failure response
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"An error occurred: {str(e)}"
        }

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
