# credit to prepare_splits

import argparse
import os
import csv
import json
import pathlib

import gdal
import tensorflow as tf
import numpy as np

# Spectral band names to read related GeoTIFF files
band_names = ['B01', 'B02', 'B03', 'B04', 'B05',
              'B06', 'B07', 'B08', 'B8A', 'B09', 'B11', 'B12']

def fix_incomplete_data(band, dimension):
    return np.pad(np.ravel(band), (0, ((dimension*dimension) - len(np.ravel(band)))))

def prep_example(bands, labels, labels_multi_hot, patch_name):
    return tf.train.Example(
            features=tf.train.Features(
                feature={
                    'B01': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B01'], 20))),
                    'B02': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B02'], 120))),
                    'B03': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B03'], 120))),
                    'B04': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B04'], 120))),
                    'B05': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B05'], 60))),
                    'B06': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B06'], 60))),
                    'B07': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B07'], 60))),
                    'B08': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B08'], 120))),
                    'B8A': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B8A'], 60))),
                    'B09': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B09'], 20))),
                    'B11': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B11'], 60))),
                    'B12': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=fix_incomplete_data(bands['B12'], 60))),
                    'BigEarthNet-19_labels': tf.train.Feature(
                        bytes_list=tf.train.BytesList(
                            value=[i.encode('utf-8') for i in labels])),
                    'BigEarthNet-19_labels_multi_hot': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=labels_multi_hot)),
                    'patch_name': tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[patch_name.encode('utf-8')]))
                }))
    
def create_tfrecord(directory):
    root_folder = f"{directory}/patches/"
    patch_names = os.listdir(root_folder)

    TFRecord_writer = tf.python_io.TFRecordWriter(f"{directory}/record.tfrecord")
    progress_bar = tf.contrib.keras.utils.Progbar(target = len(patch_names))
    for patch_idx, patch_name in enumerate(patch_names):
        patch_folder_path = os.path.join(root_folder, patch_name)
        bands = {}
        for band_name in band_names:
            # First finds related GeoTIFF path and reads values as an array
            band_path = os.path.join(
                patch_folder_path, patch_name + '_' + band_name + '.tif')
            
            band_ds = gdal.Open(band_path,  gdal.GA_ReadOnly)
            raster_band = band_ds.GetRasterBand(1)
            band_data = raster_band.ReadAsArray()
            bands[band_name] = np.array(band_data)
        
        BigEarthNet_19_labels = []
        BigEarthNet_19_labels_multi_hot = np.zeros(19,dtype=int)
        
        example = prep_example(
            bands,
            BigEarthNet_19_labels,
            BigEarthNet_19_labels_multi_hot,
            patch_name
        )
        TFRecord_writer.write(example.SerializeToString())
        progress_bar.update(patch_idx)
    TFRecord_writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'This script creates TFRecord files for the BigEarthNet-19 running')
    parser.add_argument('-d', '--dir', dest = 'directory',
                        help = 'dir path')
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()
    
    create_tfrecord(absolute_dir)
    