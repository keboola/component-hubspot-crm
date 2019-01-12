FROM python:3.7.1-slim
ENV PYTHONIOENCODING utf-8

COPY . /code/

RUN apt-get update && apt-get install -y build-essential
#RUN pip install flake8

RUN pip3 install -r /code/requirements.txt

WORKDIR /code/


CMD ["python", "-u", "/code/src/component.py"]
