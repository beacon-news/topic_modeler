from domain.query_config import QueryConfig
from domain.article import Article, ArticleTopic
from domain.topic import Topic
from utils import log_utils
from repository.repository import ArticleRepository, TopicRepository
import logging
from elasticsearch import Elasticsearch, exceptions, helpers

class ElasticsearchRepository(ArticleRepository, TopicRepository):

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
    self.article_index = "articles"
    self.topic_index = "topics"

    # TODO: secure with TLS
    # TODO: add some form of auth
    self.log.info(f"connecting to Elasticsearch at {conn}")
    self.es = Elasticsearch(conn, basic_auth=(user, password), ca_certs=cacerts, verify_certs=verify_certs)

    self.__assert_articles_index()

  def __assert_articles_index(self):
    try:
      self.log.info(f"creating/asserting index '{self.article_index}'")
      self.es.indices.create(index=self.article_index, mappings={
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
                # keyword mapping needed so we can do aggregations
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "embeddings": {
                "type": "dense_vector",
                "dims": 384, # depends on the embeddings model
              },
              "entities": {
                "type": "text"
              },
            }
          },
          "article": {
            "properties": {
              "id": {
                "type": "keyword",
              },
              "url": {
                "type": "keyword",
              },
              "metadata": {
                "properties": {
                  "source": {
                    "type": "text",
                    # keyword mapping needed so we can do aggregations
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "categories": {
                    "type": "text",
                    # keyword mapping needed so we can do aggregations
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              },
              "publish_date": {
                "type": "date",
              },
              "image": {
                "type": "keyword",
                "enabled": "false", # don't index image urls
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
        self.log.info(f"index {self.article_index} already exists")
    
    # TODO: assert topics index
  
  def get_articles(self, query: QueryConfig) -> list[Article]:
    es_query = {
      "bool": {
        "filter": {
          "range": {
            "article.publish_date": {
              "gte": query.publish_date.start.isoformat(),
              "lte": query.publish_date.end.isoformat(),
            }
          }
        }
      }
    }

    self.log.info(f"running topic modeling with query {query}")

    # query the db, take only what is needed

    q = {
      "query": es_query
    }
    result = helpers.scan(
      client=self.es,
      query=q,
      size=1000,
    )

    count = 0
    articles = []
    for doc in result:
      articles.append(Article(
        id=doc["_id"],
        url=doc["_source"]["article"]["url"],
        publish_date=doc["_source"]["article"]["publish_date"],
        author=doc["_source"]["article"]["author"],
        title=doc["_source"]["article"]["title"],
        paragraphs=doc["_source"]["article"]["paragraphs"],
        embeddings=doc["_source"]["analyzer"]["embeddings"],
      ))
      count += 1
      if count >= query.limit:
        self.log.info(f"limit {query.limit} reached, stopping scan")
        break
    
    return articles

  def store_topics(self, topics: list[Topic]) -> list[str]:
    ids = []
    self.log.info(f"attempting to insert {len(topics)} docs in {self.topic_index}")
    for ok, action in helpers.streaming_bulk(self.es, self.__generate_topic_actions(topics)):
      if not ok:
        self.log.error(f"failed to bulk store topic: {action}")
        continue
      ids.append(action["index"]["_id"])
      self.log.debug(f"successfully stored topic: {action}")
    return ids

  def __generate_topic_actions(self, topics: list[Topic]):
    for topic in topics:

      representative_articles = [{
        "_id": art.id,
        "url": art.url,
        "publish_date": art.publish_date.isoformat(),
        "author": art.author,
        "title": art.title,
      } for art in topic.representative_articles]

      action = {
        "_index": self.topic_index,
        "_id": topic.id,
        "create_time": topic.create_time.isoformat(),
        "query": {
          "publish_date": {
            "start": topic.query.publish_date.start.isoformat(),
            "end": topic.query.publish_date.end.isoformat(),
          }
        },
        "topic": topic.topic,
        "count": topic.count,
        "representative_articles": representative_articles, 
      }
      yield action
  
  def update_article_topic(self, art: Article, topic: ArticleTopic):
    # TODO: use this a compiled script, not as an inline one
    self.es.update(index=self.article_index, id=art.id, body={
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
          "topic_id": topic.id,
          "topic": topic.topic,
        }
      }
    })