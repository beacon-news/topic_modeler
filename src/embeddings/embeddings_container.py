import pickle
from datetime import date

# Contains the embeddings model

class EmbeddingsModelContainer: pass

class EmbeddingsModelContainer:

  def __init__(self,embeddings_model, embeddings_model_name):
    self.embeddings_model = embeddings_model
    self.embeddings_model_name = embeddings_model_name

  def save(self, filename):
    self.save_date = date.today()

    d = {
        "save_date": self.save_date,
        "embeddings_model": self.embeddings_model,
        "embeddings_model_name": self.embeddings_model_name,
    }
    with open(filename, 'wb') as f:
      pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
      print(f"saved {self.__class__.__name__} at {filename}")

  @classmethod
  def load(cls, filename) -> EmbeddingsModelContainer:
    with open(filename, 'rb') as f:
      print(f"loading {cls.__name__} from {filename}")
      d = pickle.load(f)
      save_date = d['save_date']
      embeddings_model = d['embeddings_model']
      embeddings_model_name = d['embeddings_model_name']
      ec = EmbeddingsModelContainer(embeddings_model, embeddings_model_name)
      ec.save_date = save_date
      print(f"loaded {cls.__name__} from {filename}")
      return ec