from elasticsearch import Elasticsearch, exceptions
import json, subprocess

# load environment config and set path vars
file_path = os.path.realpath(__file__)
directory_path = "/".join(file_path.split("/")[:-1])
with open(f"{directory_path}/environment.json") as reader:
    environment = json.load(reader)

# setup indexes
data_host = environment["elasticsearch_url"]
ceda_host = "jasmin-es1.ceda.ac.uk"

data_index = "fyp-tiles"

data_es = Elasticsearch([
    {"host": data_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
])

ceda_es = Elasticsearch([
    {"host": ceda_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
])

# create index if doesn't exist
mapping = {
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_shape"
            }
        }
    }
}
data_es.indices.create(index=data_index, ignore=400, body=mapping)

# ceda-eo query
query = {
    "query": {
        "bool": {
            "must": [
                {"match_phrase": { "misc.platform.Mission": "SENTINEL-2" } },
                {"match_phrase": { "file.location": "on_disk" } },
                {"range": { "temporal.start_time": { "gte": "2019-05-01" } } }
            ]
        }
    }
}

def get_cloud(path):
    subprocess.call(f"unzip -p {path} */GRANULE/*/MTD_TL.xml >MTD_TL.xml", shell=True)
    cloudy_xml = subprocess.check_output(["grep", "CLOUDY", "MTD_TL.xml"])
    return float(cloudy_xml.decode("utf-8").split(">")[1].split("<")[0])

# Process and reindex matches
def reindex(hits):
    for hit in hits:
        exists_query = {"query": {"terms": {"_id": [hit["_id"]]}}}
        if data_es.search(index=data_index, body=exists_query)["hits"]["total"]:
            continue
        short = {}
        short["path"] = f'{hit["_source"]["file"]["directory"]}/{hit["_source"]["file"]["data_file"]}'
        short["cloud_cover"] = get_cloud(short["path"])
        short["size"] = hit["_source"]["file"]["data_file_size"]
        short["datetime"] = hit["_source"]["temporal"]["start_time"]
        short["location"] = hit["_source"]["spatial"]["geometries"]["full_search"]

        yield { "index": { "_index" : data_index, "_id" : hit["_id"] } }
        yield short


# Initial scroll
scroll_time = "10m"
data = ceda_es.search(index="ceda-eo", body=query, scroll=scroll_time, size=50, timeout="1m")

# Get the scroll ID
sid = data["_scroll_id"]
scroll_size = len(data["hits"]["hits"])

while scroll_size > 0:
    print(f"Scrolling: {sid}")
    try:
        data_es.bulk(index=data_index, body=reindex(data["hits"]["hits"]))
    except exceptions.RequestError as e:
        print(e)
    
    data = ceda_es.scroll(scroll_id=sid, scroll=scroll_time)

    # Update the scroll ID
    sid = data["_scroll_id"]

    # Get the number of results that returned in the last scroll
    scroll_size = len(data["hits"]["hits"])
