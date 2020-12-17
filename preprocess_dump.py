""" Wikidata Dump Processor

This script preprocesses the raw Wikidata dump (in JSON format) and sorts triples into 8 "tables": labels, descriptions, aliases, entity_rels, external_ids, entity_values, qualifiers, and wikipedia_links. See the README for more information on each table. 

Example command: 

python3 preprocess_dump.py \ 
    --input_file /lfs/raiders8/0/lorr1/wikidata/raw_data/latest-all.json \
    --out_dir data/processed

"""
import argparse
import glob
import multiprocessing
import os
import shutil
import time
import ujson

from math import ceil
from tqdm import tqdm

from utils import *

# names of tables
TABLE_NAMES = ['labels', 'descriptions', 'aliases', 'external_ids', 'entity_values', 'qualifiers', 'wikipedia_links', 'entity_rels']

# properties which encode some alias/name
ALIAS_PROPERTIES = set([
    'P138', 'P734', 'P735', 'P742', 'P1448', 'P1449', 'P1477', 'P1533', 'P1549', 'P1559', 'P1560', 'P1635', 'P1705', 'P1782', 'P1785', 'P1786', 'P1787', 'P1810', 'P1813', 'P1814', 'P1888', 'P1950', 'P2358', 'P2359', 'PP2365', 'P2366', 'P2521', 'P2562', 'P2976', 'PP3321', 'P4239', 'P4284', 'P4970', 'P5056', 'P5278', 'PP6978', 'P7383'])

# data types in wikidata dump which we ignore
IGNORE = set(['wikibase-lexeme', 'musical-notation', 'globe-coordinate', 'commonsMedia', 'geo-shape', 'wikibase-sense', 'wikibase-property', 'math', 'tabular-data'])

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type = str, required = True, help = 'path to raw wikidata json dump')
    parser.add_argument('--out_dir', type = str, required = True , help = 'path to output directory')
    parser.add_argument('--language_id', type = str, default = 'en', help = 'language identifier')
    parser.add_argument('--total_lines', type = int, default = 77117245, help = 'number of lines in wikidata dump file -- userful to tracking progress.')
    parser.add_argument('--num_processes', type = int, default = 90, help = "number of concurrent processes to spin off. ")
    parser.add_argument('--batch_size', type = int, default = 10000)
    parser.add_argument('--test', action='store_true', help = 'Test run that terminates after 1 batch is processed. Useful for debugging.')
    return parser 

def process_mainsnak(data, args):
    
    datatype = data['datatype']
    if datatype == 'string':
        return data['datavalue']['value']
    elif datatype == 'monolingualtext':
        if data['datavalue']['value']['language'] == args.language_id:
            return data['datavalue']['value']['text']
    elif datatype == 'quantity':
        return data['datavalue']['value']['amount']
    elif datatype == 'time':
        return data['datavalue']['value']['time']
    elif datatype == 'wikibase-item':
        return data['datavalue']['value']['id']
    elif datatype == 'external-id':
        return data['datavalue']['value']
    elif datatype == 'url':
        return data['datavalue']['value']
    
    # we ignore all other triples
    elif datatype in IGNORE:
        return None 
    else:
        append_to_jsonl_file([data], os.path.join(args.out_dir, "errors", f"errors{multiprocessing.current_process().pid}.jsonl"))
    
    return None

def process_batch(input_args):
    batch_id, list_lines = input_args
    all_triples = {}
    total_lines = 0
    for line in list_lines:
        try:
            if len(line) == 0:
                continue
            obj = ujson.loads(line)
            total_lines += 1
            triples = triplify(obj, args_global)
            for k in triples:
                if k not in all_triples:
                    all_triples[k] = []
                all_triples[k].extend(triples[k])
        except Exception as e:
            print("Error", e)
            continue
    batch_write_data(all_triples, batch_id, args_global)
    return total_lines

def triplify(obj, args):
    out_data = {name: [] for name in TABLE_NAMES}
    # skip properties
    if obj['type'] == 'property':
        return {}
    id = obj['id'] # The canonical ID of the entity.
    # extract labels 
    label = ""
    if args.language_id in obj['labels']:
        label =  obj['labels'][args.language_id]['value']
        out_data['labels'].append({
            'qid': id,
            'label': label
        })
        out_data['aliases'].append({
            'qid': id,
            'alias': label
        })
    
    # extract description 
    description = ""
    if args.language_id in obj['descriptions']:
        description = obj['descriptions'][args.language_id]['value']
        out_data['descriptions'].append({
            'qid': id,
            'description': description,
        })
    
    # extract aliases 
    if args.language_id in obj['aliases']:
        for alias in obj['aliases'][args.language_id]:
            out_data['aliases'].append({
                'qid': id,
                'alias': alias['value'],
            })
            
    # extract english wikipedia sitelink -- we just add this to the external links table 
    sitelink = ""
    if f'{args.language_id}wiki' in obj['sitelinks']:
        sitelink = obj['sitelinks'][f'{args.language_id}wiki']['title']
        out_data['wikipedia_links'].append({
            'qid': id, 
            'wiki_title': sitelink
        })
         
    # extract claims and qualifiers
    for property_id in obj['claims']:
        for claim in obj['claims'][property_id]:
            if not claim['mainsnak']['snaktype'] == 'value':
                continue
            claim_id = claim['id']
            datatype = claim['mainsnak']['datatype']
            value = process_mainsnak(claim['mainsnak'], args)

            if value is None: 
                continue 
                
            if datatype == 'wikibase-item':
                out_data['entity_rels'].append({
                    'claim_id': claim_id,
                    'qid': id, 
                    'property_id': property_id,
                    'value': value
                }) 
            elif datatype == 'external-id':
                out_data['external_ids'].append({
                    'claim_id': claim_id,
                    'qid': id, 
                    'property_id': property_id,
                    'value': value
                }) 
            else: 
                out_data['entity_values'].append({
                    'claim_id': claim_id,
                    'qid': id, 
                    'property_id': property_id,
                    'value': value
                })
                if property_id in ALIAS_PROPERTIES: 
                    out_data['aliases'].append({
                        'qid': id,
                        'alias': value,
                    })
            
            # get qualifiers 
            if 'qualifiers' in claim:
                for qualifier_property in claim['qualifiers']:
                    for qualifier in claim['qualifiers'][qualifier_property]:
                        if not qualifier['snaktype'] == 'value':
                            continue
                        qualifier_id = qualifier['hash']
                        value = process_mainsnak(qualifier, args)
                        if value is None: 
                            continue 
                        out_data['qualifiers'].append({
                            'qualifier_id': qualifier_id,
                            'claim_id': claim_id,
                            'property_id': qualifier_property,
                            'value': value
                        })
            
    return out_data 

def write_data(triples, line_counts, batch_counts, args):
    tables = triples.keys()
    for table in tables:
        # reached batch size limit, reset line count and increment batch number
        if line_counts[table] >= args.batch_size:
            line_counts[table] = 0
            batch_counts[table] += 1

        batch = batch_counts[table]
        out_file = f"{args.out_dir}/{table}/{batch}.jsonl"
        append_to_jsonl_file(triples[table], out_file)
        line_counts[table] += len(triples[table])

def batch_write_data(triples, batch_id, args):
    tables = triples.keys()
    for table in tables:
        out_file = f"{args.out_dir}/{table}/{batch_id}.jsonl"
        append_to_jsonl_file(triples[table], out_file)

def init_func(args):
    global args_global
    args_global = args

def main():
    start = time.time()
    args = get_arg_parser().parse_args()
    print(f"ARGS: {args}")

    # check that output file exists -- create it if it doesn't
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    
    # make a folder for each table
    for table in TABLE_NAMES:
        table_dir = os.path.join(args.out_dir, table)
        if not os.path.exists(table_dir):
            os.makedirs(table_dir)
        else: 
            shutil.rmtree(table_dir)
            os.makedirs(table_dir)
    
    # make a folder for errors 
    errors_dir = os.path.join(args.out_dir, "errors")
    if not os.path.exists(errors_dir):
        os.makedirs(errors_dir)
    else: 
        shutil.rmtree(errors_dir)
        os.makedirs(errors_dir)
    
    for file in glob.glob(os.path.join(args.out_dir, "errors*.jsonl")):
        print("FOUND FILE TO REMOVE", file)
        if os.path.exists(file):
            os.remove(file)

    dump_iterator = batch_line_generator(args.input_file, args.batch_size)
    total_lines = 0

    pool = multiprocessing.Pool(processes=args.num_processes,
                                initializer=init_func,
                                initargs=[
                                    args
                                ])

    for line_cnt in tqdm(pool.imap_unordered(process_batch, dump_iterator, chunksize=1), total=int(ceil(args.total_lines/args.batch_size))):
        total_lines += line_cnt
        if args.test:
            break

    print(f"Finished processing {total_lines} in {time.time()-start}s")


if __name__ == "__main__":
    main()