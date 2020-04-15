import argparse
import glob, os, re, pathlib, subprocess, shutil, json

import prepare_data
import evaluate

# Process tile
def process_tile(directory):
    # Load metadata.json 
    with open(f"{directory}/metadata.json") as reader:
        metadata = json.load(reader)
    # Unzip L1C into dir
    subprocess.call(f"unzip {metadata['path']} -d {directory}", shell=True)
    # sen2cor
    subprocess.call(f"/home/users/tgodfrey/Sen2Cor-02.08.00-Linux64/bin/L2A_Process --cr_only {directory}/S2*L1C*.SAFE", shell=True)
    # Get names
    level_2a_dir = glob.glob(f"{directory}/S2*L2A*")[0]
    level_2a_filename = level_2a_dir.split(".SAFE")[0].split("/")[-1]
    level_2a_filename = "_".join(level_2a_filename.split("_")[:3])
    # mkdir and retile
    pathlib.Path(f"{directory}/all").mkdir(parents=True, exist_ok=True)
    print("10m bands")
    subprocess.call(f"gdal_retile.py -ps 120 120 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R10m/*_B02_10m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 120 120 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R10m/*_B03_10m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 120 120 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R10m/*_B04_10m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 120 120 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R10m/*_B08_10m.jp2", shell=True)
    print("20m bands")
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B05_20m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B06_20m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B07_20m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B8A_20m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B11_20m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 60 60 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R20m/*_B12_20m.jp2", shell=True)
    print("60m bands")
    subprocess.call(f"gdal_retile.py -ps 20 20 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R60m/*_B01_60m.jp2", shell=True)
    subprocess.call(f"gdal_retile.py -ps 20 20 -of GTiff -targetDir {directory}/all/ {directory}/S2*L2A*/GRANULE/*/IMG_DATA/R60m/*_B09_60m.jp2", shell=True)

    # Now reshuffle to dir per patch
    regex = r"^.+_(?P<band>\w+)_.+_(?P<x>\d+)_(?P<y>\d+).tif$"

    for x in range(1, 93):
        for y in range(1, 93):
            pathlib.Path(f"{directory}/patches/{level_2a_filename}_{x}_{y}").mkdir(parents=True, exist_ok=True)
            # limit
            if y > 9:
                break
        # limit
        if x > 9:
            break
    
    for filename in glob.iglob(f"{directory}/all/*"):
        filename = filename.split("/")[-1]
        match = re.search(regex, filename)
        # limit
        if int(match.group('x')) > 10:
            continue
        # limit
        if int(match.group('y')) > 10:
            continue
        new_directory = f"{level_2a_filename}_{int(match.group('x'))}_{int(match.group('y'))}"
        new_filename = f"{new_directory}_{match.group('band')}.tif"
        shutil.move(f"{directory}/all/{filename}", f"{directory}/patches/{new_directory}/{new_filename}")
    
    # Create tfrecord from patches
    prepare_data.create_tfrecord(directory)

    # Run model
    with open("/home/users/tgodfrey/fyp/fyp-scripts/config.json", "r") as f:
        model_config = json.load(f)

    model_config["tf_record_file"] = f"{directory}/record.tfrecord"

    evaluate.eval_model(model_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'This script takes a Sentinel2 tile and processes into patches and runs the model')
    parser.add_argument('-d', '--dir', dest = 'directory',
                        help = 'dir path')
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).absolute()

    process_tile(absolute_dir)