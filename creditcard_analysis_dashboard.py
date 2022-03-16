from google.cloud import bigquery
import pandas as pd

def load_cc_to_bigquery(file, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
    #     event (dict): Event payload.
    #     context (google.cloud.functions.Context): Metadata for the event.
    """
    
    bucket = file['bucket']
    filename = file['name']
    csv_uri = 'gs://{}/{}'.format(bucket, filename)
    # Use your table id below
    table_id = 'table_id_of_BigQuery_Table'
    
    print('Now reading {}'.format(csv_uri))
    data = pd.read_csv(csv_uri, header=None, names=['Transaction_Date','Posted_Date','Description','Category','Type','Amount','Memo'], skiprows=1,encoding='utf-8')
    if 'hase' in filename:
        data['Bank'] = 'Chase'
        data['Card_No'] = filename[5:9]
    
    else:
        data['Bank'] = 'Unknown'
        return
    
    data.Bank = data.Bank.astype('str')
    data.Card_No = data.Card_No.astype('str')
    data.Transaction_Date = pd.to_datetime(data.Transaction_Date)
    data.Posted_Date = pd.to_datetime(data.Posted_Date)
    data.Description = data.Description.astype('str')
    data.Category = data.Category.astype('str')
    data.Type = data.Type.astype('str')
    data.Amount = data.Amount.astype('float')
    data.Memo = data.Memo.astype('str')
 
    data = data.reindex(columns=['Bank','Card_No','Transaction_Date','Posted_Date','Description','Category','Type','Amount','Memo'])
 
    job_config = bigquery.LoadJobConfig(schema=[
                                                bigquery.SchemaField("Bank", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("Card_No", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("Transaction_Date", bigquery.enums.SqlTypeNames.DATE),
                                                bigquery.SchemaField("Posted_Date", bigquery.enums.SqlTypeNames.DATE),
                                                bigquery.SchemaField("Description", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("Category", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("Type", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("Amount", bigquery.enums.SqlTypeNames.FLOAT64),
                                                bigquery.SchemaField("Memo", bigquery.enums.SqlTypeNames.STRING),
                                                ], write_disposition="WRITE_APPEND",)

    bigquery_client = bigquery.Client()
    print("Initiated client")
    job = bigquery_client.load_table_from_dataframe(data, table_id, job_config=job_config)
    print("Loaded table")
    job.result()
    print("Got result")
    table = bigquery_client.get_table(table_id)

    print("Loaded {} rows and {} colums to {}".format(table.num_rows, len(table.schema), table_id))
 
