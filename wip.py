from __future__ import absolute_import

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.external.snowflake import ReadFromSnowflake, WriteToSnowflake
import logging

SERVER_NAME = 'polideapartner.europe-west4.gcp.snowflakecomputing.com'
USERNAME = 'PURBANOWICZ'
PASSWORD = '12QWASZX34erdfcv!'
SCHEMA = 'PUBLIC'
DATABASE = 'TEST_PAWEL'
STAGING_BUCKET_NAME = 'gcs://iot-beam-snowflake/'
STORAGE_INTEGRATION = 'iot_beam_snowflake_integration'
TABLE = 'WORDSxxx'
SCHEMA_STRING = """
{"schema":[
    {"dataType":{"type":"text","length":null},"name":"text_column","nullable":true},
    {"dataType":{"type":"integer","precision":38,"scale":0},"name":"number_column","nullable":false},
    {"dataType":{"type":"boolean"},"name":"boolean_column","nullable":false}
]}
"""


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


def run_write(pipeline_options):
    def user_data_mapper(test_row):
        return [str(test_row.text_column).encode('utf-8'),
                str(test_row.number_column).encode('utf-8'),
                str(test_row.boolean_column).encode('utf-8')
                ]

    p = beam.Pipeline(options=pipeline_options)
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


def run_read(pipeline_options):
    def csv_mapper(strings_array):
        return Row(
            strings_array[0],
            int(strings_array[1]),
            bool(strings_array[2])
        )

    def print_row(row):
        logging.error("HELLO")
        logging.error(row)
        print(row)

    p = beam.Pipeline(options=pipeline_options)
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


def run(argv=None, save_main_session=True):

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    run_write(pipeline_options)
    run_read(pipeline_options)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()