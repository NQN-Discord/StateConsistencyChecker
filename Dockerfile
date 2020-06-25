FROM python:3.7

COPY requirements.txt requirements.txt

RUN pip install -U pip wheel setuptools \
 && pip install -r requirements.txt

ADD . .

CMD python main.py