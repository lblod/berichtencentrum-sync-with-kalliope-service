# berichtencentrum-sync-with-kalliope-service

## Installation
To add the service to your stack, add the following snippet to `docker-compose.yml`:
```
    image: lblod/berichtencentrum-sync-with-kalliope-service:latest
    environment:
      KALLIOPE_API_USERNAME: "***"
      KALLIOPE_API_PASSWORD: "***"
      KALLIOPE_PS_UIT_ENDPOINT: "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-uit"
      KALLIOPE_PS_IN_ENDPOINT: "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-in"
      RUN_INTERVAL: 5
    volumes:
      - ./data/files:/data/files
```

## Configuration

### Environment variables

Required environment variables:

* `KALLIOPE_API_USERNAME`  
* `KALLIOPE_API_PASSWORD`
* `KALLIOPE_PS_UIT_ENDPOINT`
* `KALLIOPE_PS_IN_ENDPOINT`


Optional environment variables:

* `MU_APPLICATION_GRAPH`
* `MU_SPARQL_ENDPOINT`
* `MU_SPARQL_UPDATEPOINT`

* `RUN_INTERVAL`: How frequent the service to poll the API must run (in minutes), _default: 5_
* `MAX_MESSAGE_AGE`: Max age of the messages requested to the API (in days), _default: 3_. This value could theoretically be equal to that of `RUN_INTERVAL`, but a margin is advised to take eventual application or API downtime into account (to not miss any older messages).
* `MAX_SENDING_ATTEMPTS`: How many times the service can attempt to send out a certain message, _default: 3_. Prevents the API from getting the same request (that it won't accept) over and over again.

## Usage

Note that this service relies on the message-property `schema:dateReceived` not being set for finding messages that still need to be sent via the Kalliope API.