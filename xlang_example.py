import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.io.external.snowflake import ReadFromSnowflake, WriteToSnowflake
import logging

SERVER_NAME = <SNOWFLAKE SERVER NAME>
USERNAME = <SNOWFLAKE USERNAME>
PASSWORD = <SNOWFLAKE PASSWORD>
SCHEMA = <SNOWFLAKE SCHEMA>
DATABASE = <SNOWFLAKE DATABASE>
STAGING_BUCKET_NAME = <SNOWFLAKE STORAGE INTEGRATION NAME>
STORAGE_INTEGRATION = <SNOWFLAKE STORAGE INTEGRATION NAME>
TABLE = <SNOWFLAKE TABLE NAME>
SCHEMA_STRING = """
{"schema":[
    {"dataType":{"type":"text","length":null},"name":"text_column","nullable":true},
    {"dataType":{"type":"integer","precision":38,"scale":0},"name":"number_column","nullable":false},
    {"dataType":{"type":"boolean"},"name":"boolean_column","nullable":false}
]}
"""

OPTIONS =[
    "--runner=DataflowRunner",
    "--project=<GCP PROJECT ID>",
    "--staging_location=gs://<BUCKET NAME>/tmp/",
    "--region=<REGION>",
    "--temp_location=gs://<BUCKET NAME>/tmp/"
]

class Row(object):
    def __init__(self, text_column, number_column, boolean_column):
        self.text_column = text_column
        self.number_column = number_column
        self.boolean_column = boolean_column

    def __eq__(self, other):
        return self.text_column == other.text_column and \
               self.number_column == other.number_column and \
               self.boolean_column == other.boolean_column

    def __str__(self):
        return self.text_column + " " + str(self.number_column) + " " + str(self.boolean_column)

def run_write():
    def user_data_mapper(test_row):
        return [str(test_row.text_column).encode('utf-8'),
                str(test_row.number_column).encode('utf-8'),
                str(test_row.boolean_column).encode('utf-8')
                ]

    p = beam.Pipeline(options=PipelineOptions(OPTIONS))
    (p
     | GenerateSequence(start=1, stop=3)
     | beam.Map(lambda num: Row("test" + str(num), num, True))
     | "Writing into Snowflake" >> WriteToSnowflake(
                server_name=SERVER_NAME,
                username=USERNAME,
                password=PASSWORD,
                schema=SCHEMA,
                database=DATABASE,
                staging_bucket_name=STAGING_BUCKET_NAME,
                storage_integration=STORAGE_INTEGRATION,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="TRUNCATE",
                table_schema=SCHEMA_STRING,
                user_data_mapper=user_data_mapper,
                table=TABLE,
                query=None)
     )
    result = p.run()
    result.wait_until_finish()

def run_read():
    def csv_mapper(strings_array):
        return Row(
            strings_array[0],
            int(strings_array[1]),
            bool(strings_array[2])
        )

    def print_row(row):
        print(row)

    p = beam.Pipeline(options=PipelineOptions(OPTIONS))
    (p
     | "Reading from Snowflake" >> ReadFromSnowflake(
                server_name=SERVER_NAME,
                username=USERNAME,
                password=PASSWORD,
                schema=SCHEMA,
                database=DATABASE,
                staging_bucket_name=STAGING_BUCKET_NAME,
                storage_integration_name=STORAGE_INTEGRATION,
                csv_mapper=csv_mapper,
                table=TABLE)
     | "Print" >> beam.Map(print_row)
     )
    result = p.run()
    result.wait_until_finish()


def run():
    # run_write()
    run_read()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
