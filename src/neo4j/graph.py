"""Graph database wrapper"""

from neo4j import GraphDatabase
from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
    TransientError,
    ClientError,
)
from dotenv import load_dotenv
import os
import pandas as pd
import time
import logging
from models import TGChannel, ForwardedMessage

load_dotenv()

logger = logging.getLogger(__name__)


class TGChannelInsertionFailed(Exception):
    """Raised when TGChannel couldn't be inserted into the DB"""

    pass


class TGChannelAlreadyExists(Exception):
    """Raised when TGChannel already exists in the DB"""

    pass


class ForwardedMessageInsertionFailed(Exception):
    """Raised when Forwarded Message couldn't be inserted into the DB"""

    pass


class ConstraintCreationFailed(Exception):
    """Raised when creating unique constraint for channel id failed"""

    pass


class ForwardRelationInsertionFailed(Exception):
    """Raised when Relation of Forwarded Message couldn't be inserted into the DB"""

    pass


# @timeout(60 * 60)
def run_query_with_timeout(sess, query, **kwargs):
    with sess.begin_transaction() as tx:
        result = tx.run(query, **kwargs)
        data = result.data()
        tx.commit()
        return pd.DataFrame(data)


class Graph:
    """Graph database wrapper"""

    def __init__(self, uri: str = "bolt://localhost:17687"):
        self.uri = uri
        self.user = os.getenv("NEO4J_USER")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver(uri=uri, auth=(self.user, self.password))

    def create_tgc_constraint(self) -> None:
        with self.driver.session() as session:
            session.write_transaction(
                self._create_tgc_constraint,
            )

    @staticmethod
    def _create_tgc_constraint(tx) -> None:
        # use syntax of cypher 4
        channel_id_constraint_creation = """CREATE CONSTRAINT channel_id_constraint IF NOT EXISTS
                                            ON (tgc:TGChannel) ASSERT (tgc.ch_id) IS UNIQUE"""

        # use syntax of cypher 5 # noqa
        # """CREATE CONSTRAINT channel_id_constraint IF NOT EXISTS # noqa
        #                                     FOR (tgc:tgchannel) # noqa
        #                                     REQUIRE (tgc.ch_id) IS UNIQUE""" # noqa
        try:
            tx.run(channel_id_constraint_creation)
        except Exception as e:
            raise ConstraintCreationFailed(
                f"{channel_id_constraint_creation} raised an error: \n {e}"
            )

    def drop_tgc_constraint(self) -> None:
        with self.driver.session() as session:
            session.write_transaction(
                self._drop_tgc_constraint,
            )

    @staticmethod
    def _drop_tgc_constraint(tx) -> None:
        drop_channel_id_constraint_query = (
            "DROP CONSTRAINT channel_id_constraint IF EXISTS"
        )
        try:
            tx.run(drop_channel_id_constraint_query)
        except Exception as e:
            raise ConstraintCreationFailed(
                f"{drop_channel_id_constraint_query} raised an error: \n {e}"
            )

    def __repr__(self) -> str:
        return f"Graph(uri={self.uri}, user={self.user})"

    def create_tgchannel(self, tgchannel: TGChannel) -> None:
        with self.driver.session() as session:
            tgchannel = session.write_transaction(
                self._create_tgchannel,
                tgchannel,
            )
            return tgchannel

    @staticmethod
    def _create_tgchannel(
        tx,
        tgchannel: TGChannel,
    ) -> int:
        select_tgchannel_query = (
            "MATCH (tgc:tgchannel) where tgc.ch_id = $ch_id RETURN tgc AS tgchannel"
        )
        channel_exists = False
        ch_id = tgchannel.ch_id

        try:
            result = tx.run(
                select_tgchannel_query,
                ch_id=ch_id,
            )

            if result and result.single():
                channel_exists = True
        except Exception as e:
            raise TGChannelInsertionFailed(
                f"{select_tgchannel_query} raised an error: \n {e}"
            )

        if channel_exists:
            raise TGChannelAlreadyExists(f"Channel with id '{ch_id}' already exists.")

        create_channel_query = """CREATE (ch:TGChannel) SET ch.ch_id = $ch_id,
                                ch.description = $description, ch.level = $level,
                                ch.n_subscriber = $n_subscribers, ch.scam = $scam,
                                ch.title = $title, ch.username = $username,
                                ch.verified = $verified RETURN id(ch) AS id"""
        try:
            result = tx.run(
                create_channel_query,
                ch_id=ch_id,
                creation_date=tgchannel.creation_date,
                description=tgchannel.description,
                level=tgchannel.level,
                n_subscribers=tgchannel.n_subscribers,
                scam=tgchannel.scam,
                title=tgchannel.title,
                username=tgchannel.username,
                verified=tgchannel.verified,
            )
            channel_id = result.single()["id"]
        except ClientError as e:
            raise TGChannelInsertionFailed(
                f"{create_channel_query} raised an error: \n {e}"
            )

        return channel_id

    def create_forwarded_message(self, forwarded_message: ForwardedMessage) -> None:
        with self.driver.session() as session:
            forwarded_msg = session.write_transaction(
                self._create_forwarded_message,
                forwarded_message,
            )
            return forwarded_msg

    @staticmethod
    def _create_forwarded_message(tx, forwarded_message: ForwardedMessage) -> int:
        msg_id = forwarded_message.msg_id

        create_fmessage_query = """CREATE (fmsg:forwarded_message) SET fmsg.msg_id = $msg_id,
                                fmsg.author = $author, fmsg.ch_id = $ch_id,
                                fmsg.date = $date, fmsg.dst = $dst,
                                fmsg.edge_type = $edge_type, fmsg.extension = $extension,
                                fmsg.forwarded_from_id = $forwarded_from_id,
                                fmsg.forwarded_message_date = $forwarded_message_date,
                                fmsg.is_forwarded = $is_forwarded, fmsg.media_id = $media_id,
                                fmsg.note = $note, fmsg.src = $src, fmsg.title = $title RETURN id(fmsg) AS id"""

        try:
            result = tx.run(
                create_fmessage_query,
                msg_id=msg_id,
                author=forwarded_message.author,
                ch_id=forwarded_message.ch_id,
                date=forwarded_message.date,
                dst=forwarded_message.dst,
                edge_type=forwarded_message.edge_type,
                extension=forwarded_message.extension,
                forwarded_from_id=forwarded_message.forwarded_from_id,
                forwarded_message_date=forwarded_message.forwarded_message_date,
                is_forwarded=forwarded_message.is_forwarded,
                media_id=forwarded_message.media_id,
                note=forwarded_message.note,
                src=forwarded_message.src,
                title=forwarded_message.title,
            )
            inserted_id = result.single()["id"]
        except ClientError as e:
            raise ForwardedMessageInsertionFailed(
                f"{create_fmessage_query} raised an error: \n {e}"
            )

        create_forwarded_from_relation_query = "MATCH (tgc:TGChannel) WHERE tgc.ch_id=$forwarded_from_id MATCH (fmsg:forwarded_message) WHERE id(fmsg)=$inserted_id CREATE (tgc)-[r:FORWARDED_FROM]->(fmsg)"
        create_forwarded_to_relation_query = "MATCH (tgc:TGChannel) WHERE tgc.ch_id=$ch_id MATCH (fmsg:forwarded_message) WHERE id(fmsg)=$inserted_id CREATE (fmsg)-[r:FORWARDED_TO]->(tgc)"

        try:
            tx.run(
                create_forwarded_from_relation_query,
                forwarded_from_id=forwarded_message.forwarded_from_id,
                inserted_id=inserted_id,
            )
        except ClientError as e:
            raise ForwardRelationInsertionFailed(
                f"{create_forwarded_from_relation_query} raised an error: \n {e}"
            )

        try:
            tx.run(
                create_forwarded_to_relation_query,
                ch_id=forwarded_message.ch_id,
                inserted_id=inserted_id,
            )
        except ClientError as e:
            raise ForwardRelationInsertionFailed(
                f"{create_forwarded_to_relation_query} raised an error: \n {e}"
            )

    def query(self, query: str, **kwargs) -> pd.DataFrame:
        """Query the graph database.

        Args:
            query (str):
                The Cypher query.
            **kwargs:
                Keyword arguments to be used in self.driver.session().run.

        Returns:
            Pandas DataFrame: The results from the database.
        """
        restart_counter = 0
        with self.driver.session() as sess:
            while True:
                try:
                    return run_query_with_timeout(sess, query, **kwargs)

                except (TimeoutError, StopIteration):
                    print("Timed out. Rebooting graph database...")
                    os.system("docker container restart neo4j")
                    time.sleep(5 * 60)

                except (
                    ServiceUnavailable,
                    OSError,
                    SessionExpired,
                    TransientError,
                ) as e:
                    raise e
                    restart_counter += 1
                    time.sleep(1)
