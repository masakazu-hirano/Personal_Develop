FROM python:3.11.3

COPY . /usr/local/src
WORKDIR /usr/local/src

RUN apt-get update && apt-get install -y libwebp-dev ffmpeg

RUN pip install --upgrade pip \
  && pip install -r requirements.txt
