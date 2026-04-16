FROM node:25-alpine
ARG VERSION
ENV VERSION=$VERSION

WORKDIR /usr/src/app

COPY package* ./

RUN npm install

COPY src .
COPY webpack.config.js .

## Run the build step and then delete everything which only gets used for the build
RUN npm run build

RUN rm -rf node_modules client service-worker webpack*
RUN npm install --omit=dev

ENV NODE_ENV production

CMD [ "npm", "start" ]