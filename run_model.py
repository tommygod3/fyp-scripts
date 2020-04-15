# credit to eval.py

import numpy as np
import subprocess, os
import argparse, pathlib
from BigEarthNet import BigEarthNet
from utils import get_metrics
import json
import importlib
from sklearn.preprocessing import MultiLabelBinarizer
from shapely.geometry import Point
from geopandas import GeoSeries, GeoDataFrame
import tensorflow as tf
from collections import deque
from elasticsearch import Elasticsearch, exceptions, helpers



def run_and_index(directory, metadata={}):
    with open("/home/users/tgodfrey/fyp/fyp-scripts/config.json", "r") as f:
        config = json.load(f)
    
    if not metadata:
        with open(f"{directory}/metadata.json") as reader:
            metadata = json.load(reader)
    # setup patches index
    tommy_host = "search-tommygod3-es-b46x7xorl7h6jqnisw5ruua63y.eu-west-2.es.amazonaws.com"
    tommy_index = "fyp-patches"
    tommy_es = Elasticsearch([
        {"host": tommy_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
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

    deque(helpers.parallel_bulk(client=tommy_es, actions=get_data(directory, metadata, tommy_index, config), chunk_size=500), maxlen=0)

def get_data(directory, metadata, index_name, config):
    with tf.Session() as sess:
        iterator = BigEarthNet(
            f"{directory}/record.tfrecord",
            config["batch_size"],
            1, 
            0,
            config["label_type"]
        ).batch_iterator
        iterator_ins = iterator.get_next()

        model = importlib.import_module("models." + config["model_name"]).DNN_model(config["label_type"])
        model.create_network()

        variables_to_restore = tf.global_variables()
        sess.run(tf.global_variables_initializer())
        sess.run(tf.local_variables_initializer())

        model_saver = tf.train.Saver(max_to_keep=0, var_list=variables_to_restore)
        model_file = config["model_file"]
        model_saver.restore(sess, model_file)

        graph=tf.get_default_graph()
        prediction=graph.get_tensor_by_name("Cast:0")

        mlb = MultiLabelBinarizer(config["labels"])
        mlb.fit(config["labels"])

        batch_dict = sess.run(iterator_ins)
            
        sess_res = sess.run([prediction], feed_dict=model.feed_dict(batch_dict))

        results = mlb.inverse_transform(sess_res[0])

        for index, patch in enumerate(batch_dict["patch_name"].values):
            if results[index]:
                data = {}
                data.update(metadata)
                data["patch"] = patch.decode("utf-8")
                data["labels"] = results[index]
                data["location"] = patch_location(directory, data["patch"])

                yield {
                    "index": {
                        "_index": index_name
                    }
                }
                yield data

def patch_location(directory, patch_name):
    patch_dir = f"{directory}/patches/{patch_name}"
    subprocess.call(f"gdal_polygonize.py {patch_dir}/{patch_name}_B02.tif {patch_dir}/location", shell=True, stdout=subprocess.DEVNULL)
    shapefile = GeoSeries.from_file(f"{patch_dir}/location/out.shp")
    coordinates = shapefile.to_crs(epsg=4326)
    bbox = coordinates.total_bounds
    p1 = Point(bbox[0], bbox[3])
    p2 = Point(bbox[2], bbox[3])
    p3 = Point(bbox[2], bbox[1])
    p4 = Point(bbox[0], bbox[1])
    np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
    np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
    np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
    np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])
    coordinates = [np1, np2, np3, np4]
    return {"type": "Polygon", "orientation": "counterclockwise", "coordinates": coordinates}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        "This script evaluates TFRecords")
    parser.add_argument("-d", "--dir", dest = "directory",
                        help = "dir path")
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()

    run_and_index(absolute_dir)
