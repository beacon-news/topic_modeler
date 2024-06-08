from utils import log_utils
from domain.query_config import QueryConfig
from domain.article import Article, ArticleTopic
from domain.topic import Topic, TopicArticle, TopicBatch
from repository.repository import *
from utils.check_env import check_env
import hashlib

log = log_utils.create_console_logger("TopicModelerSetup")

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

log.info("initializing BERTopic dependencies")

EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
__em = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))

__umap_model = UMAP(
    n_neighbors=15, # global / local view of the manifold default 15
    n_components=5, # target dimensions default 5
    metric='cosine',
    min_dist=0.0 # smaller --> more clumped embeddings, larger --> more evenly dispersed default 0.0
)

__hdbscan_model = HDBSCAN(
    min_cluster_size=10, # nr. of points required for a cluster (documents for a topic) default 10
    metric='euclidean',
    cluster_selection_method='eom',
    prediction_data=True, # if we want to approximate clusters for new points
)

__vectorizer_model = CountVectorizer(
    ngram_range=(1, 1),
    stop_words='english',
)

__mmr = MaximalMarginalRelevance(diversity=0.3)
__representation_model = __mmr

# Note: this cannot be private because it is used in a static method, and names get mangled somehow
_bt = BERTopic(
    embedding_model=__em.ec.embeddings_model,
    umap_model=__umap_model,
    hdbscan_model=__hdbscan_model,
    vectorizer_model=__vectorizer_model,
    representation_model=__representation_model,
)

pd.set_option('display.max_columns', None)

class BertopicTopicModeler:

  def __init__(self, bertopic: BERTopic, topic_repo: TopicRepository, article_repo: ArticleRepository):
    self.__bertopic = bertopic 
    self.__topic_repo = topic_repo 
    self.__article_repo = article_repo 
    self.log = log_utils.create_console_logger(self.__class__.__name__)
  

  def model_topics(self, query: QueryConfig, articles: list[Article]):

    if len(articles) < 2:
      self.log.info("found less than 2 articles, returning")
      return

    doc_texts = ["\n".join(art.title) + "\n" + "\n".join(art.paragraphs) for art in articles]
    doc_embeddings = np.array([art.embeddings for art in articles])

    self.log.info(f"fitting topic modeling model on {len(doc_texts)} docs")

    self.__bertopic.fit_transform(doc_texts, doc_embeddings)

    topic_info = self.__bertopic.get_topic_info()
    self.log.info(f"found {len(topic_info)} topics, printing topic info")
    # must use print here because of pandas' table output
    print(topic_info)

    if (len(topic_info) == 1):
      self.log.info("found only 1 topic, the outliers, returning")
      return

    topic_batch = self.__store_topic_batch(query, topic_info)

    self.log.info(f"number of all non-outlier articles in topic batch: {topic_batch.article_count}, {topic_batch.article_count / len(articles):.2f}")

    doc_info = _bt.get_document_info(doc_texts)
    _, articles_topics_to_update = self.__store_topics(topic_batch, topic_info, doc_info, articles)

    self.__update_articles_with_topics(articles_topics_to_update)


  def __store_topic_batch(self, query: QueryConfig, topic_info: pd.DataFrame) -> TopicBatch:
    self.log.info(f"making topic batch for {len(topic_info) - 1} topics")

    start_time = query.publish_date.start.isoformat()
    end_time = query.publish_date.end.isoformat()
    batch_id = hashlib.sha1(f"{start_time}-{end_time}".encode()).hexdigest()

    # counts without the outliers
    article_count = topic_info.loc[topic_info["Topic"] != -1, ["Count"]].sum()
    topic_count = len(topic_info) - 1

    # article and topic counts will be updated after the topics have been created
    topic_batch = TopicBatch(
      id=batch_id,
      article_count=article_count,
      topic_count=topic_count, 
      query=query,
      create_time=datetime.now().isoformat(),
    )

    self.log.info(f"storing topic batch {topic_batch}")
    id = self.__topic_repo.store_topic_batch(topic_batch)
    topic_batch.id = id
    self.log.info(f"stored topic batch with id {id}")

    return topic_batch

  def __store_topics(
    self, 
    topic_batch: TopicBatch,
    topic_info: pd.DataFrame, 
    doc_info: pd.DataFrame, 
    articles: list[dict],
  ) -> tuple[list[Topic], list[tuple[dict, Topic]]]:
    """
    Returns the stored topics and a list of tuples with 
    the original document and the topic it has been assigned.
    """

    topics: list[Topic] = []
    start_time = topic_batch.query.publish_date.start
    end_time = topic_batch.query.publish_date.end

    topic_df_dict = topic_info.loc[topic_info["Topic"] != -1, ["Count", "Representation"]].to_dict()
    for d in zip(topic_df_dict["Count"].values(), topic_df_dict["Representation"].values()):

      topic_name = " ".join(d[1])
      topic_id = hashlib.sha1(f"{topic_name}-{start_time}-{end_time}".encode()).hexdigest()
      topic = Topic(
        id=topic_id,
        batch_id=topic_batch.id,
        batch_query=topic_batch.query,
        create_time=datetime.now().isoformat(),
        topic=topic_name,
        count=d[0],
        representative_articles=[],
      )
      topics.append(topic)

    self.log.info(f"found {len(topics)} topics, adding representative docs")

    articles_topics_to_update = []

    # add representative docs to topics
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

      # add topic to the article
      article_topic = ArticleTopic(
        id=topics[topic_ind].id,
        topic=topics[topic_ind].topic,
      )
      articles_topics_to_update.append((art, article_topic))

    # insert the topics
    topic_ids = self.__topic_repo.store_topics(topics)
    self.log.info(f"stored {len(topic_ids)} topics, topic ids: {topic_ids}")

    return topics, articles_topics_to_update

  def __update_articles_with_topics(self, articles_topics_to_update: list[tuple[dict, Topic]]):
    # TODO: this update/reindex could surely be done in bulk
    self.log.info(f"updating {len(articles_topics_to_update)} articles with topics")
    for art, topic in articles_topics_to_update:
      self.__article_repo.update_article_topic(art, topic)
    self.log.info("updated articles with topics")


class BertopicTopicModelerFactory:

  @staticmethod
  def create(topic_repo: TopicRepository, article_repo: ArticleRepository):
    return BertopicTopicModeler(bertopic=_bt, topic_repo=topic_repo, article_repo=article_repo)