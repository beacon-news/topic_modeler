import pydantic
from datetime import datetime
from domain.query_config import QueryConfig

# subset of article
class TopicArticle(pydantic.BaseModel):
  id: str
  url: str
  publish_date: datetime
  author: list[str]
  title: list[str]


class Topic(pydantic.BaseModel):
  id: str
  create_time: datetime
  query: QueryConfig
  topic: str
  count: int
  representative_articles: list[TopicArticle]
