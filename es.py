from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
import time

endpoint = ''
source_index = ''

session = boto3.Session()
credentials = session.get_credentials()
auth = AWSV4SignerAuth(credentials, 'eu-west-1', 'es')
client = OpenSearch(
    hosts=[{'host': endpoint.replace('https://', ''), 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=60
)
def pull_data(client, source):
    body = {
        "size": 10000,
        "query": {
            "match_all": {}
        },
        "sort": [
            {'_id': "asc"}
        ]
    }
    document_count = 0
    last_sort_value = None
    start_time = time.perf_counter()

    while True:
        if last_sort_value is not None:
            body['search_after'] = [last_sort_value]

        response = client.search(index=source, body=body)
        hits = response['hits']['hits']
        document_count += len(hits)
        if document_count % 100000 == 0:
            print(f'{document_count} documents have been retrieved')
        if not hits:
            break

        if len(hits) > 0:
            last_sort_value = hits[-1]['sort'][0]
        else:
            break

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f'Total documents pulled: {document_count}. Total time: {total_time:.2f} seconds')

if __name__ == "__main__":
    pull_data(client, source_index)