FROM nginx:1.31.1-alpine
ARG VERSION
ENV VERSION=$VERSION

RUN rm /etc/nginx/conf.d/*
RUN rm /usr/share/nginx/html/*

COPY routing.conf /etc/nginx/conf.d/
COPY resources /etc/nginx/conf.d/

CMD ["nginx", "-g", "daemon off;"]
