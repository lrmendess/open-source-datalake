# Download hive dependencies using maven

FROM maven:3.3-jdk-8 AS mvn-build

WORKDIR /usr/src/hive/

COPY ./config/hive/pom.xml .

RUN mvn clean dependency:copy-dependencies

# Preparing hive metastore

FROM apache/hive:3.1.3 AS hive

COPY ./config/hive/hive-site.xml $HIVE_HOME/conf/

COPY --from=mvn-build /usr/src/hive/lib/*.jar $HIVE_HOME/lib/

EXPOSE 9083
