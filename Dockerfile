FROM python:3

WORKDIR /usr/src/app

RUN apt-get update
RUN apt-get install -y jq awscli

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "./s3-diag.sh" ]
