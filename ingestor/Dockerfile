FROM python:3.15.0a2-alpine3.22

RUN echo "15 04 * * * cd `pwd` && /usr/local/bin/pipenv --quiet run python -u ingest.py >> /var/log/cron.log 2>&1" | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY *.py .

CMD [ "./startup.sh"]