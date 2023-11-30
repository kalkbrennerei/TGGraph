from typing import Dict
from neo4j import Record


class TGChannel:
    """
    Defines the data structure for storing and working with Telegram Channels.
    """

    def __init__(
        self,
        ch_id: int,
        creation_date: int,
        description: str,
        level: int,
        n_subscribers: int,
        scam: bool,
        title: str,
        username: str,
        verified: bool,
    ):
        self.ch_id = ch_id
        self.creation_date = creation_date
        self.description = description
        self.level = level
        self.n_subscribers = n_subscribers
        self.scam = scam
        self.title = title
        self.username = username
        self.verified = verified

    @staticmethod
    def fromDatabaseRecord(record: Record):
        """
        Initializes a new instance of a TGChannel using a Neo4J Database Record.

        :param record: A Neo4J Database Record
        :type record: neo4j.Record

        :return: A new instance of a TGChannel
        :rtype: TGChannel
        """
        tgchannel = TGChannel(
            ch_id=record["tgchannel"].id,
            creation_date=record["tgchannel"]._properties.get("creation_date"),
            description=record["tgchannel"]._properties.get("description"),
            level=record["tgchannel"]._properties.get("level"),
            n_subscribers=record["tgchannel"]._properties.get("n_subscribers"),
            scam=record["tgchannel"]._properties.get("scam"),
            title=record["tgchannel"]._properties.get("title"),
            username=record["tgchannel"]._properties.get("username"),
            verified=record["tgchannel"]._properties.get("verified"),
        )

        return tgchannel

    def toJSON(self) -> Dict:
        """
        Serializes an existing instance of a TGChannel into JSON format.

        :return: A TGChannel serialized as JSON
        :rtype: Dict
        """
        return {
            "ch_id": self.ch_id,
            "creation_date": self.creation_date,
            "description": self.description,
            "level": self.level,
            "n_subscribers": self.n_subscribers,
            "scam": self.scam,
            "title": self.title,
            "username": self.username,
            "verified": self.verified,
        }
    
class ForwardedMessage:
    """
    Defines the data structure for storing and working with Forwarded Telegram Messages.
    """

    def __init__(
        self,
        msg_id: int,
        author: str,
        ch_id: int,
        date: int,
        dst: int,
        edge_type: str,
        extension: str,
        forwarded_from_id: int,
        forwarded_message_date: int,
        is_forwarded: bool,
        media_id: int,
        note: str,
        src: str,
        title: str,
    ):
        self.msg_id = msg_id
        self.author = author
        self.ch_id = ch_id
        self.date = date
        self.dst = dst
        self.edge_type = edge_type
        self.extension = extension
        self.forwarded_from_id = forwarded_from_id
        self.forwarded_message_date = forwarded_message_date
        self.is_forwarded = is_forwarded
        self.media_id = media_id
        self.note = note
        self.src = src
        self.title = title

    @staticmethod
    def fromDatabaseRecord(record: Record):
        """
        Initializes a new instance of a ForwardedMessage using a Neo4J Database Record.

        :param record: A Neo4J Database Record
        :type record: neo4j.Record

        :return: A new instance of a ForwardedMessage
        :rtype: ForwardedMessage
        """
        forwarded_msg = ForwardedMessage(
            msg_id=record["forwarded_message"].id,
            author=record["forwarded_message"]._properties.get("author"),
            ch_id=record["forwarded_message"]._properties.get("ch_id"),
            date=record["forwarded_message"]._properties.get("date"),
            dst=record["forwarded_message"]._properties.get("dst"),
            edge_type=record["forwarded_message"]._properties.get("edge_type"),
            extension=record["forwarded_message"]._properties.get("extension"),
            forwarded_from_id=record["forwarded_message"]._properties.get("forwarded_from_id"),
            forwarded_message_date=record["forwarded_message"]._properties.get("forwarded_message_date"),
            is_forwarded=record["forwarded_message"]._properties.get("is_forwarded"),
            media_id=record["forwarded_message"]._properties.get("media_id"),
            note=record["forwarded_message"]._properties.get("note"),
            src=record["forwarded_message"]._properties.get("src"),
            title=record["forwarded_message"]._properties.get("title"),
        )

        return forwarded_msg

    def toJSON(self) -> Dict:
        """
        Serializes an existing instance of a ForwardedMessage into JSON format.

        :return: A ForwardedMessage serialized as JSON
        :rtype: Dict
        """
        return {
            "msg_id": self.msg_id,
            "author": self.author,
            "ch_id": self.ch_id,
            "date": self.date,
            "dst": self.dst,
            "edge_type": self.edge_type,
            "extension": self.extension,
            "forwarded_from_id": self.forwarded_from_id,
            "forwarded_message_date": self.forwarded_message_date,
            "is_forwarded": self.is_forwarded,
            "media_id": self.media_id,
            "note": self.note,
            "src": self.src,
            "title": self.title,
        }