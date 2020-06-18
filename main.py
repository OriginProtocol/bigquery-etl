import envkey
import logging
import os
import signal
import tempfile
import threading
import time

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from web3 import Web3
from sqlalchemy.engine.url import URL as SqlUrl
from sqlalchemy.sql import func

from ethereumetl.jobs.export_origin_job import ExportOriginJob
from ethereumetl.jobs.exporters.origin_exporter import origin_marketplace_listing_item_exporter, origin_shop_product_item_exporter
from ethereumetl.ipfs.origin import get_origin_ipfs_client
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

from google.cloud import bigquery

logging.basicConfig(level=envkey.get('LOGLEVEL', 'INFO'))

# Check for new block every 15sec.
RUN_INTERVAL_SEC = 15

# Marketplace V0 epoch.
START_BLOCK_EPOCH = 10014455 # 6436157

# Wait for 4 blocks confirmation before extracting the data.
JOB_BLOCK_LAG = 4

# Batch size for ETL job.
JOB_BLOCK_BATCH_SIZE = 100

# Number of workers for the job.
JOB_MAX_WORKERS = 5

JOB_MARKETPLACE_OUTPUT_FILENAME = tempfile.gettempdir() + '/marketplace.json'
JOB_DSHOP_OUTPUT_FILENAME = tempfile.gettempdir() + '/dshop.json'

def envkey_must_get(name):
    val = envkey.get(name)
    if not val:
        raise Exception("Env var {} must be defined".format(name))
    return val

BIGQUERY_MARKETPLACE_TABLE_ID = envkey_must_get('BIGQUERY_MARKETPLACE_TABLE_ID')
BIGQUERY_DSHOP_TABLE_ID = envkey_must_get('BIGQUERY_DSHOP_TABLE_ID')
BIGQUERY_MARKETPLACE_SCHEMA = [
    bigquery.SchemaField("block_number", "INTEGER", mode="REQUIRED", description="Ethereum block number"),
    bigquery.SchemaField("log_index", "INTEGER", mode="REQUIRED", description="Ethereum log index"),
    bigquery.SchemaField("listing_id", "STRING", mode="REQUIRED", description="Marketplace listing_id"),
    bigquery.SchemaField("ipfs_hash", "STRING", mode="REQUIRED", description="IPFS hash of the listing data"),
    bigquery.SchemaField("listing_type", "STRING", mode="NULLABLE", description="Unit, fractional, etc..."),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE", description="Unit, fractional, etc..."),
    bigquery.SchemaField("subcategory", "STRING", mode="NULLABLE", description="Sub-category"),
    bigquery.SchemaField("language", "STRING", mode="NULLABLE", description="Language"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE", description="Title"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE", description="Description"),
    bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE", description="Price"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE", description="Price currency"),
]
BIGQUERY_DSHOP_SCHEMA = [
    bigquery.SchemaField("block_number", "INTEGER", mode="REQUIRED", description="Ethereum block number"),
    bigquery.SchemaField("log_index", "INTEGER", mode="REQUIRED", description="Ethereum log index"),
    bigquery.SchemaField("listing_id", "STRING", mode="REQUIRED", description="Marketplace listing_id"),
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED", description="Unique product id"),
    bigquery.SchemaField("ipfs_path", "STRING", mode="REQUIRED", description="Location of the product data on IPFS"),
    bigquery.SchemaField("external_id", "STRING", mode="NULLABLE", description="External product id"),
    bigquery.SchemaField("parent_external_id", "STRING", mode="NULLABLE", description="External id for the parent product"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED", description="Product title"),
    bigquery.SchemaField("description", "STRING", mode="REQUIRED", description="Product description"),
    bigquery.SchemaField("price", "INTEGER", mode="REQUIRED", description="Product price"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED", description="Product price currency"),
    bigquery.SchemaField("option1", "STRING", mode="NULLABLE", description="Variant option 1"),
    bigquery.SchemaField("option2", "STRING", mode="NULLABLE", description="Variant option 2"),
    bigquery.SchemaField("option3", "STRING", mode="NULLABLE", description="Variant option 3"),
    bigquery.SchemaField("image", "STRING", mode="NULLABLE", description="Product image"),
]


# Background thread running the ETL job periodically.
class EtlThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.alive = True
        self.provider_url = envkey_must_get('PROVIDER_URL')
        self.web3 = Web3(Web3.HTTPProvider(self.provider_url))
        self._init_cursor()
        logging.info("Job marketplace output file set to {}".format(JOB_MARKETPLACE_OUTPUT_FILENAME))
        logging.info("Job dshop output file set to {}".format(JOB_DSHOP_OUTPUT_FILENAME))

    def sig_handler(self, signum, frame):
        self.alive = False

    # Helper function that allows the thread to get interrupted by sigint while sleeping.
    def _wait(self, num_sec):
        for i in range(num_sec):
            if not self.alive:
                break
            time.sleep(1)

    def run(self):
        while self.alive:
            logging.info("Running ETL job")
            self._run()
            logging.info("Sleeping for {} sec...". format(RUN_INTERVAL_SEC))
            self._wait(RUN_INTERVAL_SEC)

    # Initializes the DB and in-memory cursor.
    def _init_cursor(self):
        cursors = EtlCursor.query.all()
        if not cursors:
            # Not cursor in the DB yet. Must be our first run. Create a cursor.
            logging.info("Creating new cursor in the DB")
            cursor = EtlCursor()
            cursor.block_number = START_BLOCK_EPOCH - 1
            db.session.add(cursor)
            db.session.commit()
        else:
            cursor = cursors[0]
        self.start_block = cursor.block_number + 1

    # Sets the cursor in the DB and in memory.
    def _set_cursor(self, block_number):
        logging.info("Setting cursor to {}".format(block_number))
        self.start_block = block_number
        cursors = EtlCursor.query.all()
        if not cursors:
            raise Exception("Failed loading cursor for update")
        cursor = cursors[0]
        cursor.block_number = block_number
        db.session.add(cursor)
        db.session.commit()
        logging.info("Cursor set")

    # Run the ethereum-etl job on a range of blocks to extract marketplace and dshop data.
    def _extract(self, start_block, end_block):
        logging.info("Running extraction job for block range {}-{}".format(start_block, end_block))
        job = ExportOriginJob(
            start_block=10014454, #start_block,
            end_block=10014455, #end_block,
            batch_size=JOB_BLOCK_BATCH_SIZE,
            web3=ThreadLocalProxy(lambda: Web3(get_provider_from_uri(self.provider_url))),
            ipfs_client=get_origin_ipfs_client(),
            marketplace_listing_exporter=origin_marketplace_listing_item_exporter(JOB_MARKETPLACE_OUTPUT_FILENAME),
            shop_product_exporter=origin_shop_product_item_exporter(JOB_DSHOP_OUTPUT_FILENAME),
            max_workers=JOB_MAX_WORKERS)
        job.run()
        logging.info("Extraction done.")

    # Loads a JSON file into BigQuery. Returns the number of rows inserted.
    def _bigquery_load(self, data_type, json_data_file, schema, table_id):
        # Check the data export file. If it doesn't exist or is empty there, is nothing to do.
        data_size = os.path.getsize(json_data_file) if os.path.exists(json_data_file)  else 0
        if data_size == 0:
            logging.info("No {} data extracted. Nothing to load to BQ".format(data_type))
            return 0

        logging.info("Exporting {} data to BigQuery...".format(data_type))
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = None
        try:
            with open(json_data_file, 'rb') as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()  # Waits for table load to complete.
        except Exception as e:
            # Catch the exception to log the detailed errors, then re-raise.
            logging.error("Upload to BigQuery failed")
            if job:
                logging.error("Job errors: {}".format(job.errors))
            raise e
        logging.info("Loaded {} rows into {}.".format(job.output_rows, table_id))
        return job.output_rows

    # Load extracted marketplace json data into BigQuery.
    def _load_marketplace(self):
        return self._bigquery_load(
            'marketplace', JOB_MARKETPLACE_OUTPUT_FILENAME, BIGQUERY_MARKETPLACE_SCHEMA, BIGQUERY_MARKETPLACE_TABLE_ID)

    # Load extracted dshop json data into BigQuery.
    def _load_dshop(self):
        return self._bigquery_load(
            'dshop', JOB_DSHOP_OUTPUT_FILENAME, BIGQUERY_DSHOP_SCHEMA, BIGQUERY_DSHOP_TABLE_ID)

    def _run(self):
        # Fetch the current block number.
        try:
            block = self.web3.eth.getBlock('latest')
            latest_block_number = block['number']
            logging.debug("Current block is {}".format(latest_block_number))

            # Wait for 4 blocks confirmation before indexing the data.
            end_block = latest_block_number - JOB_BLOCK_LAG
            if end_block <= self.start_block:
                return

            # Run the extraction job. It extracts the data into local json files.
            self._extract(self.start_block, end_block)

            # Load the extracted data, if any, to BigQuery.
            self._load_marketplace()
            self._load_dshop()

            # Update the cursor.
            self._set_cursor(end_block)
        except Exception as e:
            logging.error('Run failed.', e)


# Helper function. Generates an URL to connect to Postgres.
# Uses DATABASE_URL env var if set (used for local testing).
def get_database_url():
    if os.environ.get('DATABASE_URL'):
        url = os.environ.get('DATABASE_URL')
    else:
        # See https://cloud.google.com/sql/docs/postgres/connect-app-engine-flexible#python
        db_socket_dir = os.environ.get('DB_SOCKET_DIR', "/cloudsql")
        cloud_sql_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME', 'origin-214503:us-west1:dshop-mainnet0')
        url = SqlUrl(
            drivername="postgres+pg8000",
            username=envkey_must_get('DB_USER'),
            password=envkey_must_get('DB_PASSWORD'),
            database=envkey_must_get('DB_NAME'),
            query={ "unix_sock": "{}/{}/.s.PGSQL.5432".format(db_socket_dir, cloud_sql_connection_name) }
        )
        logging.info("DB URL={}".format(url))
    return url

# Start the Flask app and run DB migrations.
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = get_database_url()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # Get rid of annoying SQL Alchemy warning at startup.

db = SQLAlchemy(app)

# DB model for the etl_cursor table.
class EtlCursor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    block_number = db.Column(db.Integer)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())
    updated_at = db.Column(db.DateTime(timezone=True), onupdate=func.now())

migrate = Migrate(app, db)

# Start the thread running the ETL cron, unless FLASK_APP is set which indicates this is just
# a flask db migrate command running.
if not os.environ.get('FLASK_APP'):
    thread = EtlThread()
    thread.start()
    # Install a signal handler to notify the thread to exit.
    signal.signal(signal.SIGTERM, thread.sig_handler)
    signal.signal(signal.SIGINT, thread.sig_handler)

@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return "Cursor: {}".format(thread.start_block)


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
