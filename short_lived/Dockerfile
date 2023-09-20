FROM python:3.8

WORKDIR /usr/src/app

COPY ./*.py ./

RUN pip install requests

VOLUME ["/usr/src/app/data"]

ENTRYPOINT ["python3", "-u", "main.py"]
