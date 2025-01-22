ARG BASE_TAG=latest

FROM debezium/connect:${BASE_TAG}

ENV CONNECTOR_NAME=debezium
ENV ENV_PREFIX=DEBEZIUM_CONFIG_

COPY build_config.py initialize.sh /setup/

CMD /setup/initialize.sh