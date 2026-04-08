FROM openjdk:22-jdk-slim

RUN apt-get update && apt-get install -y maven gettext-base curl

ENV FUSEKI_HOME=/fuseki
WORKDIR $FUSEKI_HOME

COPY pom.xml .
RUN mvn -q dependency:copy-dependencies \
    -DoutputDirectory=${FUSEKI_HOME} \
    -DincludeArtifactIds=jena-fuseki-server \
    -DexcludeTransitive=true \
    -Dmdep.stripVersion=true && \
    rm -rf ~/.m2

RUN mkdir -p run
COPY configuration ${FUSEKI_HOME}/run/configuration
COPY shiro.ini.template .

COPY startup.sh .

EXPOSE 3030

CMD ["./startup.sh"]
