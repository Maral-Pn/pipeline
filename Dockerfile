FROM python:3.10-slim

RUN mkdir -p "/usr/local/ppl"

WORKDIR "/usr/local/ppl"

RUN apt update && apt install -y git

RUN git clone git@github.com:Maral-Pn/pipeline.git

