from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
# from opensearchpy.helpers import bulk, scan
import time

endpoint = 'https://search-opensearch-1-1-production-u5l5i5yycqjbby3b2rdx74zab4.eu-west-1.es.amazonaws.com'
source_index = 'ab2f8dcc-2c23-43c4-9001-598fc2f04194'
target_index = 'pablo-test'

session = boto3.Session()
credentials = session.get_credentials()
auth = AWSV4SignerAuth(credentials, 'eu-west-1', 'es')
client = OpenSearch(
    hosts=[{'host': endpoint.replace('https://', ''), 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def perform_search():
    search_body = {
            "from": 0,
            "size": 10000,
            "query": {
                "match_all": {}
            }
        }
    
    response = client.search(
        index=source_index,
        body=search_body
    )

    return response

start_time = time.perf_counter()
search_response = perform_search()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Download completed, OpenSearch took: {search_response['took']} ms, Total Time: {elapsed_time*1000:.2f} ms")
print(len(search_response['hits']['hits']))
