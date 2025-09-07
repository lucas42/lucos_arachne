FROM alpine:3.22.1

RUN apk add --update curl bash
COPY ingest.sh .

RUN echo "15 * * * * /ingest.sh" | crontab -

CMD ["crond", "-f"]