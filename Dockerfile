FROM python:3
ADD . /code
WORKDIR /code
RUN pip install flask && \
    pip install -e .
