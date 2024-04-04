from utils import log_utils
from config.query_config import QueryConfig
from repository.elasticsearch_store import ElasticsearchStore
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

es = ElasticsearchStore(
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
    min_cluster_size=2, # nr. of points required for a cluster (documents for a topic) default 10
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

def model_topics(query: QueryConfig, docs):

  doc_texts = ["\n".join(d["article"]["title"]) + "\n" + "\n".join(d["article"]["paragraphs"]) for d in docs]
  doc_embeddings = np.array([d["analyzer"]["embeddings"] for d in docs])

  log.info(f"fitting topic modeling model on {len(doc_texts)} docs")

  bt.fit_transform(doc_texts, doc_embeddings)

  ti = bt.get_topic_info()
  log.info(f"found {len(ti)} topics, printing topic info")
  print(ti)

  # create the topics
  # create the topics without representative docs
  topics = []
  topic_info = bt.get_topic_info()

  start_time = query.publish_date.start.isoformat()
  end_time = query.publish_date.end.isoformat()

  topic_df_dict = topic_info.loc[topic_info["Topic"] != -1, ["Count", "Representation"]].to_dict()
  for d in zip(topic_df_dict["Count"].values(), topic_df_dict["Representation"].values()):
    topic_name = " ".join(d[1])
    topic_id = hashlib.sha1(f"{topic_name}-{start_time}-{end_time}".encode()).hexdigest()
    topics.append({
      "_id": topic_id, 
      "create_time": datetime.now().isoformat(),
      "query": {
        "start_time": start_time,
        "end_time": end_time,
      },
      "topic": topic_name,
      "count": d[0],
      "representative_articles": [],
    })
  
  if len(topics) == 0:
    log.info(f"only outliers found, returning")
    return
  
  log.info(f"found {len(topics)} topics, adding representative docs, updating docs with topics")

  # add docs to topics
  doc_info = bt.get_document_info(doc_texts)

  # don't filter out anything, we need the correct order to correlate with 'docs'
  doc_df_dict = doc_info.loc[doc_info["Topic"] != -1, ["Topic", "Representative_document"]].to_dict()
  for doc_ind, topic_ind, representative in zip(
    doc_df_dict["Topic"].keys(), 
    doc_df_dict["Topic"].values(), 
    doc_df_dict["Representative_document"].values()
  ):
    
    # add representative doc
    if representative:

      # add duplicate of article to topic
      art = docs[doc_ind]["article"]
      art_duplicate = {
        "_id": docs[doc_ind]["_id"],
        "url": art["url"],
        "publish_date": art["publish_date"],
        "author": art["author"],
        "title": art["title"],
      }
      topics[topic_ind]["representative_articles"].append(art_duplicate)

    es_topic = {
      "id": topics[topic_ind]["_id"],
      "topic": topics[topic_ind]["topic"],
    }
    es.update_article_topic(docs[doc_ind]["_id"], es_topic)
  
  # insert the topics
  ids = es.store_topic_batch(topics)
  log.info(f"stored {len(ids)} topics, topic ids: {ids}")
  
  print(topics[0])
  print("=======================================")
  
  # import copy
  # cp = copy.deepcopy(docs)
  # for c in cp:
  #   del c["analyzer"]["embeddings"]
  #   del c["article"]["paragraphs"]

  # print(cp[0])
  # print("=======================================")


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

  print(config)
  query = QueryConfig(**config)

  # transform query in config to db query
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

  log.info(f"running topic modeling with query {query}")

  # query the db, only what is needed
  # TODO: set a reasonable size, or process every doc
  docs = es.es.search(
    index="articles",
    query=es_query,
    source=["_id", "analyzer.embeddings", "article"],
    size=8000,
  )

  # transform 
  dt = [{
    "_id": d["_id"],
    "analyzer": {
      "embeddings": d["_source"]["analyzer"]["embeddings"],
    },
    "article": {
      **d["_source"]["article"],
    }
  } for d in docs["hits"]["hits"]]

  # run topic modeling, insert topics into db, update documents with topics
  model_topics(query, dt)

if __name__ == '__main__':
  run_topic_modeling()