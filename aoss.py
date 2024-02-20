from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from opensearchpy.helpers import bulk
import boto3

endpoint = 'https://ncv09ykby1jyu22efh3c.eu-west-1.aoss.amazonaws.com'
source_index = 'f8dc4047-9cfb-4141-8db5-ec1abfd1d48b'
target_index = 'pablo-test'

session = boto3.Session()
credentials = session.get_credentials()
auth = AWSV4SignerAuth(credentials, 'eu-west-1', 'aoss')
client = OpenSearch(
    hosts=[{'host': endpoint.replace('https://', ''), 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=60
)

def upload_documents(client, source, target, max_documents=2000000):
    body = {
        "size": 5000,
        "query": {
            "match_all": {}
        },
        "sort": [
            {'_id': "asc"}
        ]
    }
    documents_uploaded = 0
    last_sort_value = None

    while documents_uploaded < max_documents:
        if last_sort_value is not None:
            body['search_after'] = [last_sort_value]

        response = client.search(index=source, body=body)
        hits = response['hits']['hits']
        if not hits:
            break

        bulk_operations = [{
            "_index": target,
            "_op_type": "index",
            "_source": hit["_source"]
        } for hit in hits]

        bulk(client, bulk_operations)
        documents_uploaded += len(hits)
        print(f'Uploaded {len(hits)} documents, {documents_uploaded} in total.')

        if len(hits) > 0:
            last_sort_value = hits[-1]['sort'][0]
        else:
            break

client.indices.delete(index='pablo-test')
upload_documents(client, source_index, target_index)
