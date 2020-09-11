import os
import pyarrow as pa
from pyarrow import parquet as pq
from queue import Empty
import configparser
import snowflake.connector
from snowflake.connector import ProgrammingError
from tempfile import TemporaryDirectory
import random
import string


class snowflake_writer(object):

    def __init__(self, q_to_write, config_path = None):
        '''
        Takes items from `q_to_write` from a multiprocess.JoinableQueue
        and writes them to S3.

        :param multiprocessing.JoinableQueue q_to_write: Each item from the queue
            should be a dictionary containing the following information:
            - s3_bucket: str
            - key: str
            - body: pyarrow table or string
        '''
        self.q_to_write = q_to_write
        self.config_path = config_path
        # Default params for pyarrow
        self.pq_params = {
            'version': '2.0',
            'use_dictionary': True,
            'flavor': 'spark',
            'compression': 'snappy',
            'use_deprecated_int96_timestamps': True,
            'allow_truncated_timestamps': True
            }
        self.conn = self.snowflake_connection()

    def snowflake_connection(self):
        '''
        Reads from a config file and return a snowflake connection instance
        
        :param str config_path: Path to the config file to be read by configparser.
            Default ~/creds.auth
        '''
        # Read in config
        config = configparser.ConfigParser()
        config_path = self.config_path or os.path.join(os.path.expanduser('~'), 'creds.auth')
        config.read_file(open(config_path))
        pw = config.get('Snowflake', 'password')
        conn = snowflake.connector.connect(
            user = config.get('Snowflake', 'user'),
            password = config.get('Snowflake', 'password'),
            account = config.get('Snowflake', 'account')
        )
        return conn

    def listen_and_write(self, q_to_write = None):
        '''
        Fetches from a queue and writes the contents to s3.
        Items from the queue should be dictionaries containing at least:
            - s3_bucket: str
            - key: str
            - body: pyarrow table or string
        '''
        q_to_write = q_to_write or self.q_to_write

        while True:
            try:
                # Get job from queue
                job = q_to_write.get(block = False)
            except Empty:
                return
            else:
                database = job['writer_params']['snowflake_database']
                schema = job['writer_params']['snowflake_schema']
                table_name = job['writer_params']['snowflake_table']
                body = job['body']
                self.write(body, table_name, database, schema)
        return

    def write(self,
                body,
                table_name: str,
                database: str = None,
                schema: str = None,
                parallel: int = 4
        ):
        '''
        Writes content to snowflake.
        Works by loading the pyarrow tables into temporary directory,
            then uploading them and finally loading them into the table.
        An adaptation of write_pandas from https://github.com/snowflakedb/snowflake-connector-python/blob/master/src/snowflake/connector/pandas_tools.py
        :param pyarrow.Table or parquet file body: The contents to write to snowflake.
        :param str table_name: Table name where we want to insert into.
        :param str database: Database schema and table is in, if not provided the default one will be used. Default None.
        :param str schema: Schema table is in, if not provided the default one will be used. Default None.
        :param int parallel: Number of threads to be used when uploading chunks, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters (Default value = 4).
        '''

        if database is not None and schema is None:
            raise ProgrammingError("Schema has to be provided when a database is provided")
        location = ((('"' + database + '".') if database else '') + \
                    (('"' + schema + '".') if schema else '') + \
                    ('"' + table_name + '"'))

        cursor = self.conn.cursor()
        stage_name = None # Forward declaration

        # Create table
        while True:
            try:
                stage_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
                # use database
                cursor.execute(f'USE DATABASE {database}')
                create_stage_sql = ('create temporary stage /* Python:data.utils.snowflake_utils.snowflake_writer */'
                                    f'"{stage_name}"')
                print(f'creating stage with "{create_stage_sql}"')
                cursor.execute(create_stage_sql, _is_internal = True).fetchall()
                break
            except ProgrammingError as pe:
                if pe.msg.endswith('already exists'):
                    continue
                raise
        
        # Create temporary directory
        with TemporaryDirectory() as tmp_folder:
            path = os.path.join(tmp_folder, 'file.txt')
            # write body to temporary file
            if isinstance(body, pa.Table):
                pq.write_table(body,
                                path,
                                **self.pq_params
                                )
            elif isinstance(body, pd.DataFrame):
                body.to_parquet(path, compression = 'gzip')
            else:
                raise Exception(f'This format is not supported: {type(body)}')
            upload_sql = ('PUT /* Python:data.utils.snowflake_utils.snowflake_writer */'
                            '\'file://{path}\' @"{stage_name}" PARALLEL={parallel}').format(
                                path = path.replace('\\', '\\\\').replace('\'', '\\\''),
                                stage_name = stage_name,
                                parallel = parallel
                                )
            # Stage file
            print(f'Uploading files with "{upload_sql}"')
            cursor.execute(upload_sql, _is_internal = True)
            # remove file
            os.remove(path)
        
        # Copy the staged file into a table
        copy_into_sql = (f'COPY INTO {location} /* Python:data.utils.snowflake_utils.snowflake_writer */'
                        f'FROM @"{stage_name}" FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)'
                        f'MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE  PURGE=TRUE ON_ERROR=abort_statement')
        print(f'copying into with "{copy_into_sql}"')
        copy_results = cursor.execute(copy_into_sql, _is_internal = True).fetchall()
        cursor.close()

        return copy_results



def write_to_snowflake(q_to_write):
    writer = snowflake_writer(q_to_write)
    writer.listen_and_write()
    return



    # load_historic_v2_batches(['AAPL'], {'apiKey': 'AKTHU99DLS5LLD2TWFLP'}, data.utils.snowflake_utils.write_to_snowflake, {'database': 'testdb', 'schema': 'PUBLIC'}, {'start': datetime.datetime(2020,
    # ...:  9, 1), 'data_type': 'bar'}) 