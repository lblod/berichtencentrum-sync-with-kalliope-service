FROM mikidi/mu-python-template:python3-port
MAINTAINER Michaël Dierick <michael.dierick@redpencil.io>

ENV MU_APPLICATION_GRAPH "http://mu.semte.ch/graphs/public"
ENV MU_SPARQL_ENDPOINT "http://virtuoso:8890/sparql"
ENV MU_SPARQL_UPDATEPOINT "http://virtuoso:8890/sparql"
