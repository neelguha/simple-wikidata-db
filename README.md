# simple-wikidata-db

This library provides a set of scripts to download the Wikidata dump, sort it into staging files, and query the data in these staged files in a distributed manner. The staging is optimized for (1) querying time, and (2) simplicity. 

This library is helpful if you'd like to issue queries like: 

- Fetch all QIDs which are related to [Q38257](https://www.wikidata.org/wiki/Q38257)
- Fetch all triples corresponding to the relation (e.g. [P35](https://www.wikidata.org/wiki/Property:P35))
- Fetch all aliases for a QID


## Downloading the dump 

A full list of available dumps is available [here](https://dumps.wikimedia.org/wikidatawiki/entities/). To fetch the most recent dump, run: 
```
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
``` 
or, if aria2c is installed, run: 
```
aria2c --max-connection-per-server 16 https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
```

Downloading takes about 2-5 hours (depending on bandwidth).

## Processing the dump 
The original downloaded wikidata dump is a single file and combines different types of information (alias names, properties, relations, etc). We preprocess the dump by iterating over the compressed file, and saving information to different subdirectories. For more information, see the [Data Format](#data-format). To preprocess the dump, run: 

```
python3 preprocess_dump.py \ 
    --input_file $PATH_TO_COMPRESSED_WIKI_JSON \
    --out_dir $DIR_TO_SAVE_DATA_TO \
    --batch_size $BATCH_SIZE \
    --language_id $LANG
```

These arguments are:
- `input_file` (required): path to the compressed JSON Wikidata dump json file
- `out_dir` (required): path to directory where tables will be written. Subdirectories will be created under this directory for each table.
- `num_lines_read` (default: -1): number of lines to read. Useful for debuggin.
- `num_lines_in_dump` (default: -1): specifies the total number of lines in the uncompressed json file. This is used by a tqdm bar to track progress. As of January 2022, there are 95,980,335 lines in latest-all.json. It takes about ~21 minutes to run `wc -l latest-all.json`.
- `batch_size` (default: 10000): The number of triples to write into each batch file that is saved under a table directory.
- `language_id` (default `'en'`): The language to use when extracting entity labels, aliases, descriptions, and wikipedia links

To do an initial verification of the pipeline, specify a small `num_lines_read` like 100. This should finish in less than a second.

Providing `num_lines_in_dump` will provide a progress bar.

It takes ~5 hours to process the dump when running with 90 processes on a 1024GB machine with 56 cores. A tqdm progress bar should provide a more accurate estimate while data is being processed.  

## Data Format 
The Wikidata dump is made available as a single, unweildy JSON file. To make querying/filtering easier, we split the information contained in this JSON file into multiple **tables**, where each table contains a certain type of information. The tables we create are described below: 

| Table name    | Table description   | Table schema|
| --------------- |:--------------------| :-----|
| labels          | Holds the labels for different entities | qid: the QID of the entity <br> label: the entity's label ('name') |
| descriptions    | Holds the descriptions for different entities | qid: the QID of the entity <br> description: the entity's description (short summary at the top of the page) |
| aliases         | Holds the aliases for different entities  | qid: the QID of the entity <br> alias: an alias for the entity |
| entity_rels     | Holds statements where the value of the statement is another wikidata entity | claim_id: the ID for the statement <br> qid: the ID for wikidata entity <br> property_id: the ID for the property <br> value: the qid for the value wikidata entity |
| external_ids    | Holds statements where the value of the statement is an identifier to an external database (e.g. Musicbrainz, Freebase, etc) | claim_id: the ID for the statement <br> qid: the ID for wikidata entity <br> property_id: the ID for the property <br> value: the identifier for the external ID |
| entity_values   | Holds statements where the value of the statement is a string/quantity | claim_id: the ID for the statement <br> qid: the ID for wikidata entity <br> property_id: the ID for the property <br> value: the value for this property |
| qualifiers      | Holds qualifiers for statements |  qualifier_id: the ID for the qualifier <br> claim_id: the ID for the claim being qualified <br> property_id: the ID for the property <br> value: the value of the qualifier |
| wikipedia_links | Holds links to Wikipedia items | qid: the QID of the entity <br> wiki_title: link to corresponding wikipedia entity  |
----

<br><br>
Each table is stored in a directory, where the content of the table is written to multiple jsonl files stored inside the directory (each file contains a subset of the rows in the table). Each line in the file corresponds to a different triple. Partitioning the table's contents into multiple files improves querying speed--we can process each file in parallel. 


## Querying scripts 
Two scripts are provided as examples of how to write parallelized queries over the data once it's been preprocessed: 

- `fetching/fetch_with_name.py`: fetches all QIDs which are associated with a particular name. For example: all entities associated with the name 'Victoria', which would inclue entities like Victoria Beckham, or Victoria (Australia).
- `fetching/fetch_with_rel_and_value.py`: fetches all QIDs which have a relationship with a specific value. For example: all triples where the relation is P413 and the object of the relation is Q622747.

## Other helpful resources: 

- Getting the full list of properties: <https://github.com/maxlath/wikidata-properties-dumper>


**For any questions or feedback, contact Neel Guha at nguha@cs.stanford.edu**




