import json, subprocess, glob, sys, os
import pathlib, argparse

# load environment config and set path vars
file_path = os.path.realpath(__file__)
directory_path = "/".join(file_path.split("/")[:-1])
with open(f"{directory_path}/environment.json") as reader:
    environment = json.load(reader)

def process_all(top_directory):
    for directory in glob.iglob(f"{top_directory}/*"):
        subprocess.call(f"bsub -o {directory}/%J.out -W 5:00 -q short-serial {sys.executable} {directory_path}/process_tile.py -d {directory}/", shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'This script scrolls through data index and processes tiles with low cloud cover')
    parser.add_argument('-d', '--dir', dest = 'directory',
                        help = 'dir to create tiles underneath')
    args = parser.parse_args()

    absolute_dir = pathlib.Path(args.directory).resolve()

    process_all(absolute_dir)
