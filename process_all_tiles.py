import json, subprocess
import pathlib
from elasticsearch import Elasticsearch, exceptions

# setup index
tommy_host = "search-tommygod3-es-b46x7xorl7h6jqnisw5ruua63y.eu-west-2.es.amazonaws.com"
tommy_index = "fyp-tiles"
tommy_es = Elasticsearch([
    {"host": tommy_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
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
                },
                {
                    "range": {
                        "size": {
                            "gte": 500000000
                        }
                    }
                }
            ]
        }
    }
}

data = tommy_es.search(index=tommy_index, body=query, size=1000, timeout="1m")

def process_all(directory):
    for hit in data["hits"]["hits"]:
        metadata = {
            "path": hit["path"],
            "datetime": hit["datetime"]
        }
        tile_name = hit["path"].split("/")[-1].split(".zip")[0]
        pathlib.Path(f"{directory}/{tile_name}").mkdir(parents=True, exist_ok=True)
        with open(f"{directory}/{tile_name}/metadata.json", "w") as writer:
            json.dump(metadata, writer)
        subprocess.call(f"bsub -o {directory}/{tile_name}/%J.out -W 3:00 -q short-serial /home/users/tgodfrey/miniconda3/envs/fyp/bin/python /home/users/tgodfrey/fyp/fyp-scripts/process_tile.py -d {directory}/{tile_name}", shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'This script scrolls through tommy-es index and processes tiles with low cloud cover')
    parser.add_argument('-d', '--dir', dest = 'directory',
                        help = 'dir to create tiles underneath')
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()

    process_all(absolute_dir)
