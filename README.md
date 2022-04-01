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
aria2c https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
```

Downloading takes about 5 hours (depending on bandwidth). As of January 2020, uncompressed file is ~960GB. 

## Processing the dump 
The original downloaded wikidata dump is a single file and combines different types of information (alias names, properties, relations, etc). We preprocess the dump by iterating over the file, and saving information to different subdirectories. For more information, see the [Data Format](#data-format). To preprocess the dump, run: 

```
python3 preprocess_dump.py \ 
    --input_file $PATH_TO_UNCOMPRESSED_WIKI_JSON \
    --out_dir $DIR_TO_SAVE_DATA_TO \
    --batch_size $BATCH_SIZE \
    --language_id $LANG
```

These arguments are: 
- `input_file`: path to the uncompressed JSON Wikidata dump json file 
- `out_dir`: path to directory where tables will be written. Subdirectories will be created under this directory for each table. 
- `batch_size`: The number of triples to write into each batch file that is saved under a table directory. 
- `language_id`: The language to use when extracting entity labels, aliases, descriptions, and wikipedia links 

Additionally, running with the flag `--test` will terminate after processing an initial chunk, allowing you to verify results. 


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

- `fatching/fetch_with_name.py`: fetches all QIDs which are associated with a particular name. For example: all entities associated with the name 'Victoria', which would inclue entities like Victoria Beckham, or Victoria (Australia).
- `fatching/fetch_with_rel_and_value.py`: fetches all QIDs which have a relationship with a specific value. For example: all triples where the relation is P413 and the object of the relation is Q622747.


**For any questions or feedback, contact Neel Guha at nguha@cs.stanford.edu**




