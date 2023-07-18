import requests
import pandas as pd
import snowflake.connector


def lambda_handler(event=None, context=None):
    # api endpoint to collect data
    api_url = "https://data.cityofchicago.org/resource/xbjh-7zvh.json"

    try:
        # Make API request to fetch data
        response = requests.get(api_url)
        data = response.json()
        df = pd.DataFrame(data)
        
        # Connect to Snowflake
        #connection = snowflake.connector.connect(
        #account="snowflake_account",
        #user="snowflake_user",
        #password="snowflake_password",
        #warehouse="snowflake_warehouse",
        #database="snowflake_database"
        #)
        #cursor = connection.cursor()

        # create table if not exists
        #cursor.execute("""
        # CREATE TABLE IF NOT EXISTS chicago_budgets (
        #   fund_type VARCHAR,
        #   fund_code VARCHAR,
        #   fund_description VARCHAR,
        #   department_number VARCHAR,
        #   department_description VARCHAR,
        #   appropriation_authority VARCHAR,
        #   appropriation_authority_description VARCHAR,
        #   appropriation_account VARCHAR,
        #   appropriation_account_description VARCHAR, 
        #   ordinance_amount VARCHAR
        # )""")
        
        # Insert data into Snowflake table
        #df.to_sql(
        #    name='chicago_budgets',
        #    con=connection, 
        #    if_exists='replace',
        #    index=False,
        #    chunksize=10000)
        
        #connection.commit()
        #cursor.close()
        #connection.close()

        print(df.head())
        print("num of rows: ", len(data))
        print("list of keys: ",  data[0].keys())
        
        return {
            'statusCode': 200,
            'body': 'Data written to Snowflake successfully.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'Error: ' + str(e)
        }
    
if __name__ == '__main__':
    lambda_handler()