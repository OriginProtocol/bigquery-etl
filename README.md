# bigquery-etl
Job for extracting Origin Protocol's data from the blockchain and exporting it to BigQuery


## Install on local
Install python3 if it's not already installed on your host. For ex. on MacOs:

    brew install python

Create a venv environment and activate it:

    python3 -m venv ./venv
    source venv/bin/activate

## Run on local
Set PROVIDER_URL to a web3 provider. For ex:

    export PROVIDER_URL='https://eth-mainnet.alchemyapi.io/v2/<YourKey>'

Set BIGQUERY_DSHOP_TABLE_ID and BIGQUERY_MARKETPLACE_TABLE_ID to point to the target BigQuery tables. For ex.:

    export BIGQUERY_DSHOP_TABLE_ID='origin-214503.dshop.products'
    export BIGQUERY_MARKETPLACE_TABLE_ID='origin-214503.marketplace.listings'

Set DATABASE _URL to point to your local Postgres DB. For ex:

    export DATABASE_URL='postgres://origin:origin@localhost/dshop'

Using the GCP console, download the GCP credentials of the service account to use for making API calls to BigQuery as a JSON file. Then point GOOGLE_APPLICATION_CREDENTIALS=/Users/franck/src/forks/ethereum-etl/account.json
 to it. For ex.:

    export GOOGLE_APPLICATION_CREDENTIALS=/Users/franck/src/bigquery-etl/account.json

Start the app. At startup it will create the database schema if it wasn't already populated. It will expose a local server at http://127.0.0.1:8080 and will start a background thread that continuously poll sfor new blocks, extract data and publish it to BigQuery.

    gunicorn main:app

## Deploy to GCP App Engine
Decrypt the secrets to populate the .env file:

    gcloud kms decrypt --ciphertext-file=secrets.enc --plaintext-file=.env --location=global --keyring=origin --key=cloudbuild

Deploy the service to prod:

    gcloud app deploy


## Troubleshooting

### Error installing psycopg2
If the pip install of psycopg2 results in the following error:

    (error  ld: library not found for -lssl)

Set your LDFLAGS env var to the value below and retry. For example:

    env LDFLAGS='-L/usr/local/lib -L/usr/local/opt/openssl/lib -L/usr/local/opt/readline/lib' pip install  -r requirements.txt
