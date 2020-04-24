import json, subprocess, os, sys
import pathlib, argparse
from elasticsearch import Elasticsearch, exceptions

# load environment config and set path vars
file_path = os.path.realpath(__file__)
directory_path = "/".join(file_path.split("/")[:-1])
with open(f"{directory_path}/environment.json") as reader:
    environment = json.load(reader)

# setup index
data_host = environment["elasticsearch_url"]
data_index = "fyp-tiles"
data_es = Elasticsearch([
    {"host": data_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
])

query = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "cloud_cover": {
                            "lte": 15
                        }
                    }
                }
            ]
        }
    }
}

data = data_es.search(index=data_index, body=query, size=1000, timeout="1m")

def process_all(directory):
    for hit in data["hits"]["hits"]:
        metadata = {
            "path": hit["_source"]["path"],
            "datetime": hit["_source"]["datetime"]
        }
        tile_name = hit["_source"]["path"].split("/")[-1].split(".zip")[0]
        pathlib.Path(f"{directory}/{tile_name}").mkdir(parents=True, exist_ok=True)
        with open(f"{directory}/{tile_name}/metadata.json", "w") as writer:
            json.dump(metadata, writer)
        subprocess.call(f"bsub -o {directory}/{tile_name}/%J.out -W 3:00 -q short-serial {sys.executable} {directory_path}/process_tile.py -d {directory}/{tile_name}", shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'This script scrolls through data index and processes tiles with low cloud cover')
    parser.add_argument('-d', '--dir', dest = 'directory',
                        help = 'dir to create tiles underneath')
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()

    process_all(absolute_dir)
