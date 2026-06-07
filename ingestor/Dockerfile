FROM python:3.14.2-alpine

# Install supercronic v0.2.46 — a container-native cron runner that propagates the
# container environment to jobs natively, logs stdout/stderr to docker logs, and runs
# as a non-root user with standard crontab syntax.
# Update SUPERCRONIC_VERSION and the sha1sums for the corresponding release when upgrading.
# Latest releases: https://github.com/aptible/supercronic/releases
ARG TARGETARCH
RUN set -e; \
    case "$TARGETARCH" in \
        amd64) sha1sum="5bcefed628e32adc08e32634db2d10e9230dbca0" ;; \
        arm64) sha1sum="639ab81a72771990790df7ee87d9acfe88e5fa83" ;; \
        *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    wget -qO /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/v0.2.46/supercronic-linux-${TARGETARCH}"; \
    echo "${sha1sum}  /usr/local/bin/supercronic" | sha1sum -c -; \
    chmod +x /usr/local/bin/supercronic

RUN pip install pipenv

# Run jobs as a non-root user
RUN adduser -D jobrunner
USER jobrunner
WORKDIR /home/jobrunner

COPY Pipfile* ./
RUN pipenv install

ARG VERSION
ENV VERSION=$VERSION

COPY *.py .
COPY ontologies/ ontologies/
COPY crontab /crontab
COPY startup.sh .

CMD [ "./startup.sh"]
