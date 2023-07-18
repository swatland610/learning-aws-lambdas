import requests
import os
import snowflake.connector


def chicagoBudgetUpload(event, context):
    # api endpoint to collect data
    api_url = "https://data.cityofchicago.org/resource/xbjh-7zvh.json"

    try:
        # Make API request to fetch data
        response = requests.get(api_url)
        data = response.json()

        ### subset the data for testing
        test_data = data[0:10]
        
        # Connect to Snowflake
        connection = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_DEMO_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_DEMO_USER'),
        password=os.getenv('SNOWFLAKE_DEMO_PASSWORD'),
        warehouse='COMPUTE_WH',
        database='RAW',
        schema='PUBLIC'
        )
        print('connected to snowflake!')
        cursor = connection.cursor()

        # set up table name
        table_name = 'chicago_budgets'
        try: 
            # create table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS {} (
            fund_type VARCHAR,
            fund_code VARCHAR,
            fund_description VARCHAR,
            department_number VARCHAR,
            department_description VARCHAR,
            appropriation_authority VARCHAR,
            appropriation_authority_description VARCHAR,
            appropriation_account VARCHAR,
            appropriation_account_description VARCHAR, 
            ordinance_amount VARCHAR
            )""".format(table_name))
        
            for item in test_data: 
                # Insert data into Snowflake table
                columns = ', '.join(item.keys())
                columns = columns.replace('_ordinance_amount_', 'ordinance_amount')
                placeholders = ', '.join(['%s'] * len(item))
                insert_sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, columns, placeholders)

                cursor.execute(insert_sql, tuple(item.values()))
            connection.commit()
            print("Bulk insert completed successfully!")

            # close connections
            cursor.close()
            connection.close()    
            print('Data written')
        except Exception as e:
            connection.rollback()
            print(f"Error occurred during bulk insert: {str(e)}")
        

        return {
            'statusCode': 200,
            'body': 'Data written to Snowflake successfully.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'Error: ' + str(e)
        }
### UNCOMMENT TO RUN CODE LOCALLY
#if __name__ == '__main__':
#   lambda_handler(None, None)