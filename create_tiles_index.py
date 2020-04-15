from elasticsearch import Elasticsearch, exceptions
import json, subprocess

# setup indexes
tommy_host = "search-tommygod3-es-b46x7xorl7h6jqnisw5ruua63y.eu-west-2.es.amazonaws.com"
ceda_host = "jasmin-es1.ceda.ac.uk"

tommy_index = "fyp-tiles"

tommy_es = Elasticsearch([
    {"host": tommy_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
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
tommy_es.indices.create(index=tommy_index, ignore=400, body=mapping)

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
        if tommy_es.search(index=tommy_index, body=exists_query)["hits"]["total"]:
            continue
        short = {}
        short["path"] = f'{hit["_source"]["file"]["directory"]}/{hit["_source"]["file"]["data_file"]}'
        short["cloud_cover"] = get_cloud(short["path"])
        short["size"] = hit["_source"]["file"]["data_file_size"]
        short["datetime"] = hit["_source"]["temporal"]["start_time"]
        short["location"] = hit["_source"]["spatial"]["geometries"]["full_search"]

        yield {
            "index": {
                "_index": tommy_index,
                '_id': hit["_id"]
            }
        }
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
        tommy_es.bulk(index=tommy_index, body=reindex(data["hits"]["hits"]))
    except exceptions.RequestError as e:
        print(e)
    
    data = ceda_es.scroll(scroll_id=sid, scroll=scroll_time)

    # Update the scroll ID
    sid = data["_scroll_id"]

    # Get the number of results that returned in the last scroll
    scroll_size = len(data["hits"]["hits"])
