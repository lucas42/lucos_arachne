FROM python:3.13.7-alpine3.22

RUN echo "15 * * * * cd `pwd` && /usr/local/bin/pipenv --quiet run python -u ingest.py >> /var/log/cron.log 2>&1" | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY ingest.py .

CMD [ "./startup.sh"]