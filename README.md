# bigquery-etl
Job for extracting Origin Protocol's data from the blockchain and exporting it to BigQuery


## Install on local
Install python3 if it's not already installed on your host. For ex. on MacOs:

    brew install python

Create a venv environment and activate it:

    python3 -m venv ./venv
    source venv/bin/activate

Install the dependencies:

    pip install  -r requirements.txt


## Run on local
Set PROVIDER_URL to a web3 provider. For ex:

    export PROVIDER_URL='https://eth-mainnet.alchemyapi.io/v2/<YourKey>'

Set BIGQUERY_DSHOP_TABLE_ID and BIGQUERY_MARKETPLACE_TABLE_ID to point to the target BigQuery tables. For ex.:

    export BIGQUERY_DSHOP_TABLE_ID='origin-214503.dshop.products'
    export BIGQUERY_MARKETPLACE_TABLE_ID='origin-214503.marketplace.listings'

Set DATABASE _URL to point to your local Postgres DB. Note that per App Engine docs recommendation,
we use the pg8000 Python Postgres driver rather than the default SQLAlchemy psycopg2 driver.
Therefore '+pg8000' MUST be appended to the database dialect. For ex:

    export DATABASE_URL='postgres+pg8000://origin:origin@localhost/dshop'

Using the GCP console, download the GCP credentials of the service account to use
for making API calls to BigQuery as a JSON file. Then point GOOGLE_APPLICATION_CREDENTIALS
to it. Service accounts can be found on the GCP console at https://console.cloud.google.com/apis/credentials.
Click on "Credentials". In production, it uses the default App Engine service account.
For testing on local, it is preferred to use an account with more restrictive permissions
such as the "bigquery-etl" one.

    export GOOGLE_APPLICATION_CREDENTIALS=/Users/franck/src/bigquery-etl/account.json

Run the DB migrations to create the schema:

    FLASK_APP=main flask db upgrade

Start the app. At startup it will create the database schema if it wasn't already created.
It will expose a local server at http://127.0.0.1:8000 and will start a background thread
that continuously polls for new blocks, extracts data and publishes the data to BigQuery.

    python main.py
    
In prod, App Engine uses gunicorn to start the app. You can test that by running:

    gunicorn main:app

## Deploy to GCP App Engine
Decrypt the secrets to populate the .env file:

    gcloud kms decrypt --ciphertext-file=secrets.enc --plaintext-file=.env --location=global --keyring=origin --key=cloudbuild

Deploy the service to prod:

    gcloud app deploy

## Troubleshooting

### Error installing psycopg2
If you want to use the Postgres psycopg2 driver rather than pg8000, and the pip install of psycopg2 results in the following error:

    (error  ld: library not found for -lssl)

Set your LDFLAGS env var to the value below and retry. For example:

    env LDFLAGS='-L/usr/local/lib -L/usr/local/opt/openssl/lib -L/usr/local/opt/readline/lib' pip install  -r requirements.txt
