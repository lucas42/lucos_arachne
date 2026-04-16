FROM lucas42/lucos_scheduled_scripts:2.0.3
ARG VERSION
ENV VERSION=$VERSION

RUN echo "15 04 * * * INGEST_STARTUP_DELAY=0 pipenv --quiet run python -u ingest.py" | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY *.py .
COPY ontologies/ ontologies/

CMD [ "./startup.sh"]