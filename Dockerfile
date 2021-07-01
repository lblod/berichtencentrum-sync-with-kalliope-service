FROM mikidi/mu-python-template:python3-port
LABEL maintainer="info@redpencil.io"

ENV MU_APPLICATION_GRAPH "http://mu.semte.ch/graphs/public"
ENV MU_SPARQL_ENDPOINT "http://virtuoso:8890/sparql"
ENV MU_SPARQL_UPDATEPOINT "http://virtuoso:8890/sparql"

ENV RUN_INTERVAL 5
ENV MAX_MESSAGE_AGE 3
ENV MAX_SENDING_ATTEMPTS 3
ENV MAX_CONFIRMATION_ATTEMPTS 20

ADD certs /usr/local/share/ca-certificates/
RUN update-ca-certificates

RUN mkdir -p /data/files
