# Dockerfile, Image, Container
FROM python:3.9-slim-buster

WORKDIR /usr/app/src

ADD main.py .

COPY requirements.txt ./requirements.txt

COPY ./data/ ./data/

RUN pip3 install -r requirements.txt

CMD ["python", "-u","-ti", "./main.py"]

