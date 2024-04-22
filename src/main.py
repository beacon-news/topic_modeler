from utils import log_utils
from domain.query_config import QueryConfig
from domain.article import Article, ArticleTopic
from domain.topic import Topic, TopicArticle, TopicBatch
from repository.elasticsearch_repository import ElasticsearchRepository
from repository.repository import *
import os
import sys
import json
import yaml
import hashlib

log = log_utils.create_console_logger("TopicModeler")
log.info("importing BERTopic and its dependencies")

from embeddings.embeddings_container import EmbeddingsModelContainer
from embeddings.embeddings_model import EmbeddingsModel
from datetime import datetime
import numpy as np
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
from bertopic.representation import MaximalMarginalRelevance
import pandas as pd

def check_env(name: str, default=None) -> str:
  value = os.environ.get(name, default)
  if value is None:
    raise ValueError(f'{name} environment variable is not set')
  return value

EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_HOST', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))

repo: ArticleRepository | TopicRepository = ElasticsearchRepository(
  ELASTIC_CONN, 
  ELASTIC_USER, 
  ELASTIC_PASSWORD, 
  ELASTIC_CA_PATH, 
  not ELASTIC_TLS_INSECURE
)

log.info("initializing BERTopic dependencies")

em = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))

umap_model = UMAP(
    n_neighbors=15, # global / local view of the manifold default 15
    n_components=5, # target dimensions default 5
    metric='cosine',
    min_dist=0.0 # smaller --> more clumped embeddings, larger --> more evenly dispersed default 0.0
)

hdbscan_model = HDBSCAN(
    min_cluster_size=10, # nr. of points required for a cluster (documents for a topic) default 10
    metric='euclidean',
    cluster_selection_method='eom',
    prediction_data=True, # if we want to approximate clusters for new points
)

vectorizer_model = CountVectorizer(
    ngram_range=(1, 1),
    stop_words='english',
)

# ps = PartOfSpeech("en_core_web_sm")
mmr = MaximalMarginalRelevance(diversity=0.3)

# representation_model = [ps, mmr]
representation_model = mmr

bt = BERTopic(
    embedding_model=em.ec.embeddings_model,
    umap_model=umap_model,
    hdbscan_model=hdbscan_model,
    vectorizer_model=vectorizer_model,
    representation_model=representation_model,
)

pd.set_option('display.max_columns', None)

def model_topics(query: QueryConfig, articles: list[Article]):

  doc_texts = ["\n".join(art.title) + "\n" + "\n".join(art.paragraphs) for art in articles]
  doc_embeddings = np.array([art.embeddings for art in articles])

  log.info(f"fitting topic modeling model on {len(doc_texts)} docs")

  bt.fit_transform(doc_texts, doc_embeddings)

  topic_info = bt.get_topic_info()
  log.info(f"found {len(topic_info)} topics, printing topic info")
  print(topic_info)

  if (len(topic_info) == 1):
    log.info("only found 1 topic, the outliers, returning")
    return
  
  # create the topic batch once we know there are some topics
  log.info(f"making topic batch for {len(topic_info) - 1} topics")

  start_time = query.publish_date.start.isoformat()
  end_time = query.publish_date.end.isoformat()
  batch_id = hashlib.sha1(f"{start_time}-{end_time}".encode()).hexdigest()

  topic_batch = TopicBatch(
    id=batch_id,
    article_count=0,
    query=query,
    create_time=datetime.now().isoformat(),
  )

  non_outlier_count = topic_info.loc[topic_info["Topic"] != -1, ["Count"]].sum()
  log.info(f"number of all non-outlier articles in run: {non_outlier_count}, {non_outlier_count / len(articles)}")


  # create the topics without representative docs
  topics: list[Topic] = []

  topic_df_dict = topic_info.loc[topic_info["Topic"] != -1, ["Count", "Representation"]].to_dict()
  for d in zip(topic_df_dict["Count"].values(), topic_df_dict["Representation"].values()):

    topic_name = " ".join(d[1])
    topic_id = hashlib.sha1(f"{topic_name}-{start_time}-{end_time}".encode()).hexdigest()
    topic = Topic(
      id=topic_id,
      batch_id=batch_id,
      batch_query=query,
      create_time=datetime.now().isoformat(),
      topic=topic_name,
      count=d[0],
      representative_articles=[],
    )
    topics.append(topic)

    # update the article count of the batch
    topic_batch.article_count += topic.count

  
  log.info(f"found {len(topics)} topics, adding representative docs, updating docs with topics")

  # add docs to topics
  doc_info = bt.get_document_info(doc_texts)

  doc_df_dict = doc_info.loc[doc_info["Topic"] != -1, ["Topic", "Representative_document"]].to_dict()
  for doc_ind, topic_ind, representative in zip(
    doc_df_dict["Topic"].keys(), 
    doc_df_dict["Topic"].values(), 
    doc_df_dict["Representative_document"].values()
  ):
    
    art = articles[doc_ind]

    # add representative doc to the topic
    if representative:
      topics[topic_ind].representative_articles.append(
        TopicArticle(
          id=art.id,
          url=art.url,
          image=art.image,
          publish_date=art.publish_date,
          author=art.author,
          title=art.title,
        )
      )

    article_topic = ArticleTopic(
      id=topics[topic_ind].id,
      topic=topics[topic_ind].topic,
    )
    repo.update_article_topic(art, article_topic)

  # insert the topic batch
  stored_batch_id = repo.store_topic_batch(topic_batch)
  log.info(f"stored topic batch with id {stored_batch_id}")

  # insert the topics
  ids = repo.store_topics(topics)
  log.info(f"stored {len(ids)} topics, topic ids: {ids}")


def run_topic_modeling():
  env_config_path = check_env("QUERY_CONFIG", "")

  if len(sys.argv) >= 2:
    config_path = sys.argv[1]
  elif env_config_path != "":
    config_path = env_config_path
  else:
    print(f"usage: {sys.argv[0]} <config_path>, or set the 'QUERY_CONFIG' environment variable")
    exit(1)

  with open(config_path) as f:
    if config_path.endswith(".json"):
      config = json.load(f)
    elif config_path.endswith(".yaml") or config_path.endswith(".yml"):
      config = yaml.safe_load(f)

  query = QueryConfig(**config)

  articles = repo.get_articles(query)

  # run topic modeling, insert topics into db, update documents with topics
  model_topics(query, articles)

if __name__ == '__main__':
  run_topic_modeling()