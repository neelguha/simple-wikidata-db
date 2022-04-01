"""
This script fetches all QIDs which are associated with a particular name/alias (i.e. "Victoria")

to run: 
python3.6 fetch_aliases.py --data $DATA --out_dir $OUT --qid Q30
""" 

import argparse
from tqdm import tqdm 
from multiprocessing import Pool
from functools import partial 

from fetching.utils import jsonl_generator, get_batch_files

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type = str, default = 'data/processed/aliases', help = 'path to output directory')
    parser.add_argument('--name', type = str, default='Victoria', help ='name to search for')
    parser.add_argument('--num_procs', type = int, default=10, help ='Number of processes')
    return parser 


def filtering_func(target_name, filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item['alias'] == target_name:
            filtered.append(item)
    return filtered

def main():
    args = get_arg_parser().parse_args()

    table_files = get_batch_files(args.data)
    pool = Pool(processes = args.num_procs)
    filtered = []
    for output in tqdm(
        pool.imap_unordered(
            partial(filtering_func, args.name), table_files, chunksize=1), 
        total=len(table_files)
    ):
        filtered.extend(output)
    
    print(f"Extracted {len(filtered)} rows:")
    for i, item in enumerate(filtered):
        print(f"Row {i}: {item}")
    

                
    
    


if __name__ == "__main__":
    main()