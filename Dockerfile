FROM python:3.12-bookworm

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt --no-cache-dir

RUN useradd app --create-home --shell /bin/bash
USER app

COPY . .

ENV EMBEDDINGS_MODEL_PATH=models/embeddings/embeddings_container_all_MiniLM_L6_v2.pkl

ENTRYPOINT ["python3", "src/main.py"]
