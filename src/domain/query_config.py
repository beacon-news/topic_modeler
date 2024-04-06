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
    # e.g. absolute iso time: 2020-01-01T00:00:00
    # e.g. relative time: now-1d, now-1m

    try:
      dt = datetime.fromisoformat(date_str)
    except Exception:
      # try to parse relative string
      if len(re.findall(r'^now(-[0-9]+(d|w|m|y))?$', date_str)) == 0:
        raise ValueError(f"invalid date string: {date_str}")
      
      if date_str == 'now':
        return datetime.now()

      # strip the 'now-'
      date_str = date_str[4:]
      
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
