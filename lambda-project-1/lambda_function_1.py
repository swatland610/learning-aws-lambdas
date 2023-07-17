import requests
import snowflake.connector


def lambda_handler(event, context):
    # api endpoint to collect data
    api_url = "https://data.cityofchicago.org/resource/xbjh-7zvh.json"

    try:
        # Make API request to fetch data
        response = requests.get(api_url)
        data = response.json()
        
        # Connect to Snowflake
        connection = snowflake.connector.connect(
        account="snowflake_account",
        user="snowflake_user",
        password="snowflake_password",
        warehouse="snowflake_warehouse",
        database="snowflake_database"
        )
        cursor = connection.cursor()
        
        # Insert data into Snowflake table
        for item in data:
            cursor.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (item['value1'], item['value2']))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return {
            'statusCode': 200,
            'body': 'Data written to Snowflake successfully.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'Error: ' + str(e)
        }