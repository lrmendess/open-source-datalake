FROM metabase/metabase:v0.49.2

ENV TRINO_PLUGIN_VERSION="4.1.0"
ENV TRINO_PLUGIN_VERSION_JAR="starburst-${TRINO_PLUGIN_VERSION}.metabase-driver.jar"
ENV TRINO_PLUGIN_DOWNLOADS_URL="https://github.com/starburstdata/metabase-driver/releases/download"

RUN wget ${TRINO_PLUGIN_DOWNLOADS_URL}/${TRINO_PLUGIN_VERSION}/${TRINO_PLUGIN_VERSION_JAR} -O /plugins/${TRINO_PLUGIN_VERSION_JAR}
