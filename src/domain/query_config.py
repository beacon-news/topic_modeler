from datetime import datetime
from dateutil.relativedelta import relativedelta
import pydantic
import re

class PublishDateQuery(pydantic.BaseModel):
  start: datetime
  end: datetime 

  @pydantic.field_validator("start", "end", mode="before")
  def parse_date_str(date_str: str) -> datetime:

    # validates absolute and relative date strings
    # e.g. absolute iso date time: 2020-01-01T00:00:00
    # e.g. relative date time (days are the most granular option): today-1d, today-1m

    try:
      dt = datetime.fromisoformat(date_str)
    except Exception:
      # try to parse relative string
      if len(re.findall(r'^today(-[0-9]+(d|w|m|y))?$', date_str)) == 0:
        raise ValueError(f"invalid date string: {date_str}")

      today_str = 'today'
      
      if date_str == today_str:
        return datetime.today()

      # strip the 'today-'
      date_str = date_str[len(today_str) + 1:]
      
      # get the number
      m = re.match(r'[0-9]+', date_str)
      s = m.span()
      num = int(date_str[s[0]:s[1]])
      
      # parse the unit
      date_str = date_str[s[1]:]
      dt = datetime.now()
      if date_str == 'd':
        dt -= relativedelta(days=num)
      elif date_str == 'w':
        dt -= relativedelta(weeks=num)
      elif date_str == 'm':
        dt -= relativedelta(months=num)
      elif date_str == 'y':
        dt -= relativedelta(years=num)

    return dt
  
  @pydantic.model_validator(mode="after")
  def valid_dates(self):
    if self.start > self.end:
      raise ValueError(f"start date {self.start} is after end date {self.end}")
    return self


class QueryConfig(pydantic.BaseModel):
  publish_date: PublishDateQuery 
  limit: int = 8000
