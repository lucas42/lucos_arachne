FROM lucas42/lucos_scheduled_scripts:1.0.1

RUN echo "15 04 * * * pipenv --quiet run python -u ingest.py" | crontab -
COPY startup.sh .

RUN pip install pipenv
COPY Pipfile* ./
RUN pipenv install

COPY *.py .

CMD [ "./startup.sh"]