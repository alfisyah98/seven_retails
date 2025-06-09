FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && apt-get install -y

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .

CMD [ "stdbuf", "-oL", "python3", "-u", "main.py"]