
# If this was a real world application & I had unlimited resources
# other things I would have done with more time & resources / in a real world scenario:

# I would skip the json step entirely and pipeline directly in memory from hl7 messages,
# I would have written much more extensive validation to detect & deal with different HL7 standards in a uniform way.
# Streaming pipeline instead of bulk memory transformations.
# Created explicit table relationships for combined queries


import orjson
import pandas as pd
from copy import deepcopy
import os
import codecs
import time
import sqlalchemy as sa
from sqlalchemy import text


#function for if database doesn't already exist.
def create_database(db_name, con):
    con.execute(f'CREATE DATABASE {db_name}')

    print(f"Database built with name: {db_name}")

    return db_name

#cross join exploded dicts
def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows

#flatten nested lists in json
def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


#combine exploded json & lists into a neat dataframe
def json_to_df(data_in):
    def flatten_data(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_data(value, prev_heading + '.' + key))
        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_data(item, prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

    return pd.DataFrame(flatten_data(data_in))

#combine dataframes by iterating through list of files
def combine_temp_df():
    df_temp_dict = {}

    data_path = "data/"

    directory = os.fsencode(data_path)

    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        
        # here we loop through the files in the directory, turn them into dataframes and append
        # an empty dictionary with those dataframes to be combined.

        # This is not a production quality approach. here I would choose to write rows of each dataframe straight to
        # table with pyspark clusters (i'm not familiar with hadoop, but I understand that is a possible approach.)
        # or a serverless warehouse like bigquery/redshift.

        # throws exception if not json
        
        if filename.endswith(".json"):
            print(filename)
            # re-encode as UTF-8 by default, if character error will throw exception.
            with codecs.open(f"{data_path}{filename}", "r", encoding='UTF-8') as f:
                
                # Here we could also stream straight to mysql, but due to time constraints and the complexity of the
                # json schema for this data we're programmatically generating the tables rather than writing them
                # with a well-designed relational table structure for streaming.
                
                json_data = orjson.loads(f.read())
                df_temp_dict[filename] = json_to_df(json_data)

    # Hence, we're creating this giant dataframe below which slows us down significantly.
    combined_df = pd.concat(df_temp_dict.values(), ignore_index=True)

    # Example of dropping useless columns.
    combined_df.drop(columns=['resourceType', 'type', 'entry.resource.presentedForm.data'])
    for col in combined_df.columns:
        col.lstrip('entry.')

    return combined_df


def create_sql_engine():
    # connect to mysql container instance, if database doesn't exit then creates it.

    user = 'root'
    password = 'groot'
    db_ip = 'mysql:33066'
    db_ip_alt = 'mysql:3306'
    db_name = 'interview_database'

    try:
        con = sa.create_engine(f'mysql+mysqlconnector://{user}:{password}@{db_ip}/{db_name}', pool_recycle=1,
                               pool_timeout=57600).connect()
    except:
        try:
            con = sa.create_engine(f'mysql+mysqlconnector://{user}:{password}@{db_ip_alt}/{db_name}', pool_recycle=1,
                                   pool_timeout=57600).connect()
        except:
            raise Exception(f"cannot to connect to {db_name}. Check hostname, user & password.")

    try:
        create_database(db_name, con)
    except:
        print('something went wrong connecting to the database. It may already exit, if so ignore this!')

    return con

#this function takes our large in memory dataframe & splits it by 'resourceType' into a host of related tables connected by 'resource_id'.

def dataframe_splitter(df):
    user = 'root'
    password = 'groot'
    db_ip = 'mysql:3306'
    db_name = 'interview_database'

    con = sa.create_engine(f'mysql+mysqlconnector://{user}:{password}@{db_ip}/{db_name}', pool_recycle=1,
                           pool_timeout=57600).connect()

    df = df.rename(columns=lambda x: x.removeprefix('entry.'))
    df = df.rename(columns=lambda x: x.replace('.', '_'))

    # TODO The database has a many to many relationship on column "resource_id". Here I began the process to create and assign a resource_id parent table, but fell short on time.

    # id_list = pd.DataFrame(df['resource_id'].unique())
    # id_list.columns = ['resource_id']
    # id_list.to_sql(con=con, name=f'flattened_json_primary_table', if_exists='replace', index=False)

    type_list = df['resource_resourceType'].unique()

    for types in type_list:
        print(f"{types} table is loading. Name: flattened_json_{types}_table")
        df_filtered = df[(df['resource_resourceType'] == str(types))]

        df_filtered.dropna(how='all', axis=1, inplace=True)

        table_identifier = types.lower()

        df_filtered.to_sql(con=con, name=f'flattened_json_{table_identifier}_table', if_exists='replace', index=False)
        con.execute(f'''SELECT * FROM flattened_json_{table_identifier}_table LIMIT 10''')

    con.close()


def example_query():
    con = create_sql_engine()

    print("you're now ready to query a table.")

    with con.connect() as c:
        print("here is a list of tables in interview_database:")
        tables = c.execute(text(f"SHOW TABLES"))
        for x in tables:
            print(x)

        query = f"SELECT * FROM flattened_json_patient_table LIMIT 10"

        query_results = c.execute(query)

        print("here is an example query for each table:")

        example_df = pd.DataFrame(query_results)

        print("In a real world application, users would be able to connect to this docker environment & query with a mysql GUI."
              "You can use the docker commands in the README file.")


def main():
    print("pipeline running")
    start_time = time.time()

    df = combine_temp_df()
    print("Dataframe written to memory in --- %s seconds ---" % (
            time.time() - start_time) + "See comments in combine_temp_df() for how this could have been much faster.")

    start_time = time.time()

    dataframe_splitter(df)

    print("Data written to MySQL in --- %s seconds ---" % (
            time.time() - start_time) + "See comments in combine_temp_df() for how this could have been much faster.")

    example_query()


if __name__ == '__main__':
    main()
