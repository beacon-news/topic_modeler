from abc import ABC, abstractmethod
from domain.query_config import QueryConfig
from domain.article import Article
from domain.topic import Topic, ArticleTopic


class TopicRepository(ABC):

  @abstractmethod
  def store_topics(self, topics: list[Topic]) -> list[str]:
    """Store the topics, and return their ids."""
    raise NotImplementedError


class ArticleRepository(ABC):

  @abstractmethod
  def get_articles(self, query: QueryConfig) -> list[Article]:
    """Get the articles that match the query."""
    raise NotImplementedError

  @abstractmethod
  def update_article_topic(self, article: Article, topic: ArticleTopic):
    """Update the article with a subset of the full topic information."""
    raise NotImplementedError