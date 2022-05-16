FROM ghcr.io/deephaven/server

COPY secrets/google-key.json /google-key.json
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
