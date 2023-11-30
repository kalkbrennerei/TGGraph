# TGGraph
Creating a graph from the TGDataset


- run `src/zenodo-download/zenodo-download.py` to download the TGDataset data
- then follow the instructions from https://github.com/SystemsLab-Sapienza/TGDataset to populate the mongo DB
- run `src/construct_graph/get_urls_data.py`, `src/construct_graph/compute_tme_edges.py` and `src/construct_graph/compute_TG_graph.ipynb` to construct a graph
- the result will be save in `TG_edges.csv` and `TG_nodes.csv`
- run `src/neo4j/launch.sh` to start the database, then head over to the web interface to change the password (this has to be done only once)
- run `src/neo4j/populate.py` to populate the graph database from the nodes and edges csv files