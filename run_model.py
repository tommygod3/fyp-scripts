# Adapted from https://gitlab.tu-berlin.de/rsim/bigearthnet-models-tf/blob/cec5ae5eb0e55a0d15f24abd426b3158e6e8e130/eval.py

import numpy as np
import math
import subprocess, os, glob
import argparse, pathlib
from BigEarthNet import BigEarthNet
from utils import get_metrics
import json
import importlib
from sklearn.preprocessing import MultiLabelBinarizer
import tensorflow as tf
from elasticsearch import Elasticsearch, exceptions, helpers
from collections import deque
import gdal, osr
from geojson import Polygon
from geojson_rewind import rewind

# load environment config and set path vars
file_path = os.path.realpath(__file__)
directory_path = "/".join(file_path.split("/")[:-1])
with open(f"{directory_path}/environment.json") as reader:
    environment = json.load(reader)

def run_and_index(directory, metadata={}):
    if not metadata:
        with open(f"{directory}/metadata.json") as reader:
            metadata = json.load(reader)
    # setup patches index
    data_host = environment["elasticsearch_url"]
    data_index = "fyp-patches"
    data_es = Elasticsearch([
        {"host": data_host, "port": 443, "use_ssl": True, "timeout": 60, "max_retries": 10, "retry_on_timeout": True},
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

    deque(helpers.parallel_bulk(client=data_es, actions=get_data(directory, metadata, data_index), chunk_size=50), maxlen=0)

def get_data(directory, metadata, index_name):
    with open(f"{directory_path}/config.json", "r") as f:
        config = json.load(f)
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
        model_file = environment["model_weights"]
        model_saver.restore(sess, model_file)

        graph=tf.get_default_graph()
        prediction=graph.get_tensor_by_name("Cast:0")

        mlb = MultiLabelBinarizer(config["labels"])
        mlb.fit(config["labels"])

        num_patches = len(glob.glob(f"{directory}/patches/*"))
        for batch_number in range(math.ceil(num_patches / config["batch_size"])):
            try:
                batch_dict = sess.run(iterator_ins)
                sess_res = sess.run([prediction], feed_dict=model.feed_dict(batch_dict))
                results = mlb.inverse_transform(sess_res[0])
            except tf.errors.OutOfRangeError:
                pass

            for index, patch in enumerate(batch_dict["patch_name"].values):
                if results[index]:
                    data = {}
                    data.update(metadata)
                    data["labels"] = results[index]
                    data["location"] = patch_location(directory, patch.decode("utf-8"))

                    yield {
                        "_index" : "fyp-patches",
                        "_source": data
                    }

def patch_location(directory, patch_name):
    ds = gdal.Open(f"{directory}/patches/{patch_name}/{patch_name}_B01.tif")

    old_cs = osr.SpatialReference()
    old_cs.ImportFromWkt(ds.GetProjectionRef())
    wgs84_wkt = """
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]]"""
    new_cs = osr.SpatialReference()
    new_cs .ImportFromWkt(wgs84_wkt)
    
    transform = osr.CoordinateTransformation(old_cs,new_cs) 
    width = ds.RasterXSize
    height = ds.RasterYSize
    gt = ds.GetGeoTransform()
    minx = gt[0]
    miny = gt[3] + width*gt[4] + height*gt[5] 
    maxx = gt[0] + width*gt[1] + height*gt[2]
    maxy = gt[3]
    np1 = transform.TransformPoint(minx, miny)[1::-1]
    np2 = transform.TransformPoint(minx, maxy)[1::-1]
    np3 = transform.TransformPoint(maxx, maxy)[1::-1]
    np4 = transform.TransformPoint(maxx, miny)[1::-1]
    coordinates = [[np1, np2, np3, np4, np1]]
    geo_json = rewind(Polygon(coordinates))
    geo_json["orientation"] = "counterclockwise"
    return geo_json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        "This script evaluates TFRecords")
    parser.add_argument("-d", "--dir", dest = "directory",
                        help = "dir path")
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()

    run_and_index(absolute_dir)
