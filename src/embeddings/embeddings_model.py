from embeddings.embeddings_container import EmbeddingsModelContainer
import numpy as np


class EmbeddingsModel:

  def __init__(self, embeddings_container: EmbeddingsModelContainer):
    self.ec = embeddings_container

  def encode(self, docs) -> np.ndarray:
    return self.ec.embeddings_model.encode(docs)
