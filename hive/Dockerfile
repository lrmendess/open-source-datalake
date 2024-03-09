# Download hive dependencies using maven

FROM maven:3.3-jdk-8 AS mvn-build

WORKDIR /usr/src/hive/

COPY ./pom.xml .

RUN mvn clean dependency:copy-dependencies

# Preparing hive metastore image

FROM apache/hive:3.1.3 AS hive

COPY ./hive-site.xml $HIVE_HOME/conf/
COPY --from=mvn-build /usr/src/hive/lib/* $HIVE_HOME/lib/

EXPOSE 9083
