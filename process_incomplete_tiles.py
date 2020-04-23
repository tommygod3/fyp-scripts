import json, subprocess, glob
import pathlib, argparse

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
