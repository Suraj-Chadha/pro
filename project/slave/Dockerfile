FROM ubuntu:18.04

RUN apt update && \
    apt install -y python3 && \
    apt-get install -y python3-pip && \
    pip3 install sqlalchemy requests pika docker kazoo flask flask_sqlalchemy_session

ENV TEAM_NAME=CC_0007_0040_0080_0264

WORKDIR /app

COPY . /app

EXPOSE 5000

CMD ["python3","ms.py"]
