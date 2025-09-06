FROM alpine:3.22.1

RUN apk add --update curl bash
COPY ingest.sh .

CMD ["./ingest.sh"]