FROM python:3.9-buster

RUN mkdir -p /opt/app
RUN mkdir -p /opt/app/data
RUN mkdir -p /opt/app/logs

COPY requirements.txt Makefile .env /opt/app/
COPY data /opt/app/data/
COPY *.py /opt/app/

WORKDIR /opt/app
RUN python -m venv venv
RUN make env
RUN chmod 755 /opt/app/main.py
RUN chown -R www-data:www-data /opt/app/

EXPOSE 8000
EXPOSE 5432
RUN make
