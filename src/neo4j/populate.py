'''Populate the graph database'''

import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from pathlib import Path
import datetime as dt
from typing import List, Optional
import json
import re
from neo4j.exceptions import DatabaseError
from concurrent.futures import ProcessPoolExecutor

from graph import Graph
from models import TGChannel, ForwardedMessage

def load_records(path: str) -> pd.DataFrame:
    '''Helper function to load records from a CSV file'''
    try:
        df = pd.read_csv(path,
                            engine='python',
                            error_bad_lines=False,
                            warn_bad_lines=False)
        df = df.replace({np.nan: None})
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

def populate(start_from_scratch: bool = False):
    '''Populate the graph database with nodes and relations'''
    # Initialise the graph database
    graph = Graph()

    # Delete all nodes and constraints in database
    if start_from_scratch:
        graph.query('CALL apoc.periodic.iterate('
                    '"MATCH (n) RETURN n",'
                    '"DETACH DELETE n",'
                    '{batchsize:10000, parallel:false})')

        graph.drop_tgc_constraint()

    # # Set up cypher directory
    # cypher_dir = '.' / 'src' / 'neo4j' / 'cypher'
    # constraint_paths = list(cypher_dir.glob('constraint_*.cql'))

    graph.create_tgc_constraint()

    nodes = load_records("/data/tgdataset/graph/TG_nodes.csv")
    edges = load_records("/data/tgdataset/graph/TG_edges.csv")

    for idx, node in tqdm(nodes.iterrows(), total=len(nodes)):
        tgchannel = TGChannel(
        ch_id=node["ch_id"],
        creation_date=node["creation_date"],
        description=node["description"],
        level=node["level"],
        n_subscribers=node["n_subscribers"],
        scam=node["scam"],
        title=node["title"],
        username=node["username"],
        verified=node["verified"],
        )

        graph.create_tgchannel(tgchannel)

    for idx, edge in tqdm(edges.iterrows(), total=len(edges)):
        forwarded_message = ForwardedMessage(
            msg_id=edge["msg_id"],
            author=edge["author"],
            ch_id=edge["ch_id"],
            date=edge["date"],
            dst=edge["dst"],
            edge_type=edge["edge_type"],
            extension=edge["extension"],
            forwarded_from_id=edge["forwarded_from_id"],
            forwarded_message_date=edge["forwarded_message_date"],
            is_forwarded=edge["is_forwarded"],
            media_id=edge["media_id"],
            note=edge["note"],
            src=edge["src"],
            title=edge["title"],
        )

        graph.create_forwarded_message(forwarded_message)

populate(True)