# credit to eval.py

import numpy as np
import tensorflow as tf
import subprocess, time, os
import argparse
from BigEarthNet import BigEarthNet
from utils import get_metrics
import json
import importlib
from sklearn.preprocessing import MultiLabelBinarizer

def eval_model(config):
    with tf.Session() as sess:
        iterator = BigEarthNet(
            config["tf_record_file"], 
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

        eval_res = {}

        graph=tf.get_default_graph()
        prediction=graph.get_tensor_by_name("Cast:0")

        mlb = MultiLabelBinarizer(config["labels"])
        mlb.fit(config["labels"])

        batch_dict = sess.run(iterator_ins)
            
        sess_res = sess.run([prediction], feed_dict=model.feed_dict(batch_dict))

        results = mlb.inverse_transform(sess_res[0])

        for index, patch in enumerate(batch_dict["patch_name"].values):
            print(f"Patch: {patch}, labels: {results[index]}")
            


if __name__ == "__main__":
    with open("/home/users/tgodfrey/fyp/fyp-scripts/config.json", "r") as f:
        config = json.load(f)

    parser = argparse.ArgumentParser(description=
        'This script evaluates TFRecords')
    parser.add_argument('-d', '--tf_record', dest = 'tf_record',
                        help = 'filepath of tfrecord')
    args = parser.parse_args()
    config["tf_record_file"] = args.tf_record

    eval_model(config)
