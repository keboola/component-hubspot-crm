FROM python:3.7.1-alpine
ENV PYTHONIOENCODING utf-8

COPY . /code/

RUN apk add --no-cache --virtual .build-deps gcc musl-dev
RUN pip install flake8

RUN pip install -r /code/requirements.txt

WORKDIR /code/


CMD ["python", "-u", "/code/src/component.py"]
