import json
import boto3
import csv
import psycopg2

def lambda_handler(event, context):
    # Retrieve bucket name and object key from the event
    bucket_name = event['detail']['requestParameters']['bucketName']
    object_key = event['detail']['requestParameters']['key']
    print ("bucket_name:", bucket_name)
    print ("object_key:", object_key)
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Initialize Redshift connection parameters
    redshift_params = {
        'dbname': 'ecommerce_tran_data',
        'user': 'awsuser',
        'password': 'Raja5485',
        'host': 'ecommerece-data.ciltqz0dll8g.ap-south-1.redshift.amazonaws.com',
        'port': '5439'
    }

    try:
        # Get the content of the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        # Read the content of the object
        csv_data = response['Body'].read().decode('utf-8')
        
        # Parse the CSV data
        rows = csv_data.split('\n')
        csv_reader = csv.reader(rows)
        
        # Connect to Redshift
        conn = psycopg2.connect(**redshift_params)
        cursor = conn.cursor()
        
        # Iterate through each row of the CSV data
        for row in csv_reader:
            # Skip the header row
            if row[0] == 'TransactionID':
                continue
            
            # Extract values from the CSV row
            transaction_id = row[0]
            date = row[1]
            product_id = row[2]
            quantity = row[3]
            price = row[4]
            store_location = row[5]
            
            # Define the SQL query for upsert operation
            upsert_query = """
                INSERT INTO sales_transactions (TransactionID, Tran_Date, ProductID, Quantity, Price, StoreLocation)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (TransactionID)
                DO UPDATE SET
                Tran_Date = EXCLUDED.Tran_Date,
                ProductID = EXCLUDED.ProductID,
                Quantity = EXCLUDED.Quantity,
                Price = EXCLUDED.Price,
                StoreLocation = EXCLUDED.StoreLocation;
            """
            
            # Execute the SQL query with the CSV row values
            cursor.execute(upsert_query, (transaction_id, date, product_id, quantity, price, store_location))
        
        # Commit the transaction and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()
        
        # Return a success response
        return {
            'statusCode': 200,
            'body': json.dumps('Data loaded into Redshift successfully')
        }
    except Exception as e:
        # Handle any errors
        print("Error:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps('Error loading data into Redshift')
        }
