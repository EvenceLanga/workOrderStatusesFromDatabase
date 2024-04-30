import pandas as pd
import mysql.connector
from mysql.connector import Error



# Read Excel data
excel_file_path = "C:\\Users\\Evence Langa\\Downloads\\November MTD 2023.xlsx"
#sheet_name = "September 2023"  

df = pd.read_excel(excel_file_path)

# Database connection parameters
db_params = {
    'host_name': '##############',
    'user_name': '############',
    'user_password': '######################',
    'db_name': '########################',
}

# Function to create a database connection
def create_db_connection(host_name, user_name, user_password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
        )
        print("MySQL Database connection successful")
        return connection
    except Error as err:
        print(f"Error: {err}")
        return None

# Function to fetch status from database
def fetch_status(last_36_chars, connection):
    try:
        query = "SELECT status FROM work_orders WHERE guid = %s"
        params = (last_36_chars,)

        cursor = connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()

        if result:
            status = result[0]
            return {'status': status}
        else:
            return {'status': 'status not found'}

    except Error as e:
        return {'status': f"Error: {str(e)}"}

# Establish a database connection
connection = create_db_connection(**db_params)

# Check if the connection is successful
if connection:
    
    df['Last_36_Chars'] = df['WO URL'].apply(lambda url: url[-36:])

    # Iterate through Excel rows and fetch status for each Last_36_Char
    status_data = df['Last_36_Chars'].apply(lambda last_36: fetch_status(last_36, connection))

    # Create a new DataFrame with the extracted status data
    status_df = pd.DataFrame(list(status_data)) 

    # Concatenate the new DataFrame with the original DataFrame
    result_df = pd.concat([df, status_df], axis=1)

    # Save updated DataFrame to a new Excel file
    output_excel_path = 'output_file_with_status_sep.xlsx'
    result_df.to_excel(output_excel_path, index=False)

    print(f"Results saved to {output_excel_path}")

    # Close the database connection
    connection.close()
else:
    print("Database connection failed.")
