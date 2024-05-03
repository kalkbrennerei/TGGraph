# TGGraph
Creating a graph from the TGDataset

This repository contains code to create a dataset from forwarded telegram messages based on the TGDataset.
The code was created as a proof of concept for a master thesis. The current version of the project is in a private repo. If you're interested in the project, please get in touch!

## Instructions to reproduce:
- run `src/zenodo-download/zenodo-download.py` to download the TGDataset data
- then follow the instructions from https://github.com/SystemsLab-Sapienza/TGDataset to populate the mongoDB database with the TGDataset
- run `src/construct_graph/get_urls_data.py`, `src/construct_graph/compute_tme_edges.py` and `src/construct_graph/compute_TG_graph.ipynb` to construct a graph
- the result will be saved in `TG_edges.csv` and `TG_nodes.csv`
- run `src/neo4j/launch.sh` to start the database, then head over to the web interface to change the password (this has to be done only once)
- run `src/neo4j/populate.py` to populate the graph database from the nodes and edges csv files
