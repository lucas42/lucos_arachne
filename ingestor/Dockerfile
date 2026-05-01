FROM lucas42/lucos_scheduled_scripts:2.0.10
ARG VERSION
ENV VERSION=$VERSION

RUN printf '15 04 * * * INGEST_STARTUP_DELAY=0 pipenv --quiet run python -u ingest.py\n30 03 * * 0 pipenv --quiet run python -u compact.py\n' | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY *.py .
COPY ontologies/ ontologies/

CMD [ "./startup.sh"]