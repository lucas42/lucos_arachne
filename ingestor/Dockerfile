FROM lucas42/lucos_scheduled_scripts:1.0.10

RUN echo "15 04 * * * INGEST_STARTUP_DELAY=0 pipenv --quiet run python -u ingest.py" | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY *.py .

CMD [ "./startup.sh"]