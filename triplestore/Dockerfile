FROM openjdk:18-jdk-slim

# Install Fuseki from source
RUN apt-get update && apt-get install -y wget unzip gettext-base
ENV FUSEKI_HOME=/fuseki
ENV FUSEKI_VERSION=5.5.0
RUN wget https://downloads.apache.org/jena/binaries/apache-jena-fuseki-${FUSEKI_VERSION}.zip && \
    unzip apache-jena-fuseki-${FUSEKI_VERSION}.zip && \
    mv apache-jena-fuseki-${FUSEKI_VERSION} ${FUSEKI_HOME} && \
    rm apache-jena-fuseki-${FUSEKI_VERSION}.zip

WORKDIR $FUSEKI_HOME
ENV PATH=$PATH:$FUSEKI_HOME

COPY startup.sh .
RUN mkdir -p run
COPY configuration ${FUSEKI_HOME}/run/configuration
COPY shiro.ini.template .

EXPOSE 3030

# Start the Fuseki server
CMD ["./startup.sh"]
