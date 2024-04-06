import pydantic
from datetime import datetime

# take only what's needed for topic modeling
class Article(pydantic.BaseModel):
  id: str
  url: str
  publish_date: datetime
  author: list[str]
  title: list[str]
  paragraphs: list[str]
  embeddings: list[float]
