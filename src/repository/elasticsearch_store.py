from utils import log_utils
import logging
from elasticsearch import Elasticsearch, exceptions, helpers

class ElasticsearchStore:

  @classmethod
  def configure_logging(cls, level: int):
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(
      self, 
      conn: str, 
      user: str, 
      password: str, 
      cacerts: str, 
      verify_certs: bool = True,
      log_level: int = logging.INFO
  ):
    self.configure_logging(log_level)
    self.index_name = "articles"

    # TODO: secure with TLS
    # TODO: add some form of auth
    self.log.info(f"connecting to Elasticsearch at {conn}")
    self.es = Elasticsearch(conn, basic_auth=(user, password), ca_certs=cacerts, verify_certs=verify_certs)

    # assert articles index
    try:
      self.log.info(f"creating/asserting index '{self.index_name}'")
      self.es.indices.create(index=self.index_name, mappings={
        "properties": {
          "topics": {
            "properties": {
              "topic_ids": {
                "type": "keyword"
              },
              "topic_names": {
                "type": "text"
              }
            }
          },
          "analyzer": {
            "properties": {
              "categories": {
                "type": "text",
                # TODO: remove keyword mapping? it doesn't do much...
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "embeddings": {
                "type": "dense_vector",
                "dims": 384, # depends on model used
              },
              "entities": {
                "type": "text"
              },
            }
          },
          "article": {
            "properties": {
              "url": {
                "type": "keyword",
              },
              "publish_date": {
                "type": "date",
              },
              "author": {
                "type": "text",
              },
              "title": {
                "type": "text",
              },
              "paragraphs": {
                "type": "text",
              },
            }
          }
        }
      })
    except exceptions.BadRequestError as e:
      if e.message == "resource_already_exists_exception":
        self.log.info(f"index {self.index_name} already exists")
    
    # TODO: assert topics index

  def store_topic_batch(self, topics: list[dict]) -> list[str]:
    topics_index = "topics"
    ids = []
    self.log.info(f"attempting to insert {len(topics)} docs in {topics_index}")
    for ok, action in helpers.streaming_bulk(self.es, self.__generate_topic_actions(topics_index, topics)):
      if not ok:
        self.log.error(f"failed to bulk store topic: {action}")
        continue
      ids.append(action["index"]["_id"])
      self.log.debug(f"successfully stored topic: {action}")
    return ids

  def __generate_topic_actions(self, index: str, topics: list[dict]):
    for i in range(len(topics)):
      action = {
        "_index": index,
        # also contains the "_id" 
        **topics[i],
      }
      yield action
  
  def update_article_topic(self, id: str, topic: dict):
    # TODO: use this a compiled script
    self.es.update(index=self.index_name, id=id, body={
      "script": {
        "source": """
        if (ctx._source.topics == null) {
          ctx._source.topics = [
            'topic_ids': [],
            'topic_names': []
          ]
        }
        if (!ctx._source.topics.topic_ids.contains(params.topic_id)) { 
          ctx._source.topics.topic_ids.add(params.topic_id) 
        } 
        if (!ctx._source.topics.topic_names.contains(params.topic)) { 
          ctx._source.topics.topic_names.add(params.topic) 
        }
        """,
        "params": {
          "topic_id": topic["id"],
          "topic": topic["topic"],
        }
      }
    })