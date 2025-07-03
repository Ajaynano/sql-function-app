
import azure.functions as func
import logging
import psycopg2
import pandas as pd
import xlsxwriter
from azure.storage.blob import BlobServiceClient
import io
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import time


# Use a single FunctionApp instance for all routes
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger_v3")
def http_trigger_v3(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()
    logging.info('Python HTTP trigger function started.')

    azure_container_name = req.params.get('azure_container_name')
    limit = req.params.get('limit')

    # Try to get from body if not present in query params
    if not azure_container_name or not limit:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        if not azure_container_name:
            azure_container_name = req_body.get('azure_container_name')
        if not limit:
            limit = req_body.get('limit')

    # Validate parameters
    if not azure_container_name or str(azure_container_name).strip() == '':
        return func.HttpResponse("Missing or empty required parameter: azure_container_name", status_code=400)
    if not limit or str(limit).strip() == '':
        return func.HttpResponse("Missing or empty required parameter: limit", status_code=400)
    try:
        limit = int(limit)
        if limit <= 0:
            return func.HttpResponse("Parameter 'limit' must be a positive integer", status_code=400)
    except Exception:
        return func.HttpResponse("Parameter 'limit' must be an integer", status_code=400)

    # Retrieve secrets from Azure Key Vault using Managed Identity
    keyVaultName ="https://v1-dev.vault.azure.net/" or os.environ["KEY_VAULT_NAME"]
    credential = DefaultAzureCredential()
    client = SecretClient("{}".format(keyVaultName), credential=credential)
    # Key Vault and secret names (update these as needed)
    BLOB_CONNECTION_STRING = client.get_secret('SECRET-BLOB-CONN').value
    DB_HOST = client.get_secret('SECRET-DB-HOST').value
    DB_USER = client.get_secret('SECRET-DB-USER').value
    DB_PASSWORD = client.get_secret('SECRET-DB-PASS').value

    # Other config
    BLOB_CONTAINER_NAME = azure_container_name or os.environ.get('BLOB_CONTAINER_NAME', 'dataexport')
    BLOB_FILE_NAME = os.environ.get('BLOB_FILE_NAME', 'output.xlsx')
    POSTGRES_TABLE = os.environ.get('PG_TABLE', 'test_bulk_insert')  # Table to read from

    POSTGRES_CONN = {
        'host': DB_HOST or os.environ.get('PG_HOST', ''),
        'port': int(os.environ.get('PG_PORT', 5432)),
        'dbname': os.environ.get('PG_DB', 'survey_request_aj'),
        'user': DB_USER or os.environ.get('PG_USER', ''),
        'password': DB_PASSWORD or os.environ.get('PG_PASSWORD', '')
    }

    try:
        # 1. Connect to PostgreSQL and check connection
        try:
            conn = psycopg2.connect(**POSTGRES_CONN)
            conn_status = True
        except Exception as db_exc:
            logging.error(f"PostgreSQL connection failed: {db_exc}")
            return func.HttpResponse(f"PostgreSQL connection failed: {str(db_exc)}", status_code=500)

        # If connection is successful, proceed to read records with user-specified limit
        try:
            query = f"SELECT * FROM {POSTGRES_TABLE} LIMIT {limit}"
            df = pd.read_sql_query(query, conn)
            logging.info(f"Fetched {len(df)} rows from PostgreSQL with limit {limit}.")
        except Exception as fetch_exc:
            logging.error(f"Failed to fetch data from PostgreSQL: {fetch_exc}")
            return func.HttpResponse(f"Failed to fetch data from PostgreSQL: {str(fetch_exc)}", status_code=500)
        finally:
            conn.close()

        # 2. Generate Excel file with pandas and xlsxwriter, store in output.xlsx (in memory)
        file_stream = io.BytesIO()
        try:
            with pd.ExcelWriter(file_stream, engine='xlsxwriter') as writer:
                # Write the main data to the first sheet
                df.to_excel(writer, sheet_name='Data', index=False)

                # Create a pivot table on the second sheet if possible
                if len(df.columns) >= 2:
                    pivot = pd.pivot_table(df, index=[df.columns[0]], values=[df.columns[1]], aggfunc='count')
                    pivot.to_excel(writer, sheet_name='PivotTable')
                else:
                    # Write a message if not enough columns
                    pd.DataFrame({'Message': ['Not enough columns for pivot table']}).to_excel(writer, sheet_name='PivotTable', index=False)
            file_stream.seek(0)
            logging.info(f"Excel file generated in memory, size: {file_stream.getbuffer().nbytes} bytes.")
        except Exception as excel_exc:
            logging.error(f"Failed to generate Excel file: {excel_exc}")
            return func.HttpResponse(f"Failed to generate Excel file: {str(excel_exc)}", status_code=500)

        # 3. Upload the generated file to Azure Blob Storage
        try:
            blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=BLOB_FILE_NAME)
            logging.info(f"Uploading to blob: container={BLOB_CONTAINER_NAME}, blob={BLOB_FILE_NAME}")
            blob_client.upload_blob(file_stream, overwrite=True)
            logging.info("Upload to Azure Blob Storage successful.")
        except Exception as blob_exc:
            logging.error(f"Failed to upload to Azure Blob Storage: {blob_exc}")
            return func.HttpResponse(f"Failed to upload to Azure Blob Storage: {str(blob_exc)}", status_code=500)

        duration = time.time() - start_time
        minutes = int(duration // 60)
        seconds = duration % 60
        duration_str = f"{minutes}m {seconds:.2f}s"
        logging.info(f"http_trigger_v3 completed in {duration_str}.")
        return func.HttpResponse(
            f"Excel file with {len(df)} records and pivot table uploaded to blob storage as {BLOB_FILE_NAME}. Duration: {duration_str}.",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    import time
    start_time = time.time()
    logging.info('Python HTTP trigger function processed a request.')

    print("Request received.")

    name = req.params.get('name')
    print(f"Name from query params: {name}")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            print(f"Name from request body: {name}")

    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = duration % 60
    duration_str = f"{minutes}m {seconds:.2f}s"
    if name:
        print(f"Final name value: {name}")
        response = func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully. Duration: {duration_str}.")
        print(f"Return value: {response.get_body().decode()}")
        return response
    else:
        response = func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response. Duration: {duration_str}.",
             status_code=200
        )
        print(f"Return value: {response.get_body().decode()}")
        return response

@app.route(route="http_trigger2")
def http_trigger2(req: func.HttpRequest) -> func.HttpResponse:
    import time
    start_time = time.time()
    logging.info('Python HTTP trigger 2 function processed a request.')

    print("Request received for http_trigger2.")

    name = req.params.get('name')
    print(f"Name from query params: {name}")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            print(f"Name from request body: {name}")

    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = duration % 60
    duration_str = f"{minutes}m {seconds:.2f}s"
    if name:
        print(f"Final name value: {name}")
        response = func.HttpResponse(f"Hello from http_trigger2, {name}. This HTTP triggered function executed successfully. Duration: {duration_str}.")
        print(f"Return value: {response.get_body().decode()}")
        return response
    else:
        response = func.HttpResponse(
             f"This HTTP triggered function (http_trigger2) executed successfully. Pass a name in the query string or in the request body for a personalized response. Duration: {duration_str}.",
             status_code=200
        )
        print(f"Return value: {response.get_body().decode()}")
        return response