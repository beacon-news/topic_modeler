from domain.query_config import QueryConfig
from repository.elasticsearch_repository import ElasticsearchRepository
from repository.repository import *
import sys
import json
import yaml
from topic_modeling.bertopic_topic_modeler import BertopicTopicModelerFactory
from utils.check_env import check_env


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

topic_modeler = BertopicTopicModelerFactory.create(repo, repo)


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
  topic_modeler.model_topics(query, articles)

if __name__ == '__main__':
  run_topic_modeling()