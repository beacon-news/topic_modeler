import pydantic
from datetime import datetime
from domain.query_config import QueryConfig

class TopicBatch(pydantic.BaseModel):
  id: str
  query: QueryConfig
  article_count: int
  create_time: datetime


# subset of article
class TopicArticle(pydantic.BaseModel):
  id: str
  url: str
  image: str | None
  publish_date: datetime
  author: list[str]
  title: list[str]


class Topic(pydantic.BaseModel):
  id: str
  batch_id: str
  batch_query: QueryConfig
  create_time: datetime
  topic: str
  count: int
  representative_articles: list[TopicArticle]
