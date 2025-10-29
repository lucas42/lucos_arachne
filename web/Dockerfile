FROM nginx:1.29.3-alpine

RUN rm /etc/nginx/conf.d/*
RUN rm /usr/share/nginx/html/*

COPY routing.conf /etc/nginx/conf.d/
COPY resources /etc/nginx/conf.d/

CMD ["nginx", "-g", "daemon off;"]
