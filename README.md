# berichtencentrum-sync-with-kalliope-service

## Installation
To add the service to your stack, add the following snippet to `docker-compose.yml`:
```
    image: lblod/berichtencentrum-sync-with-kalliope-service:latest
    environment:
      KALLIOPE_API_USERNAME: "***"
      KALLIOPE_API_PASSWORD: "***"
      KALLIOPE_PS_UIT_ENDPOINT: "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-uit"
      KALLIOPE_PS_UIT_CONFIRMATION_ENDPOINT: "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-uit/ontvangstbevestiging"
      KALLIOPE_PS_IN_ENDPOINT: "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-in"
      BERICHTEN_CRON_PATTERN: "*/5 * * * *"
      BERICHTEN_IN_CONFIRMATION_CRON_PATTERN: "3/5 * * * *"
      INZENDINGEN_CRON_PATTERN: "* 22 * * *"
    volumes:
      - ./data/files:/data/files
```

## Configuration

### Environment variables

Required environment variables:

* `KALLIOPE_API_USERNAME`
* `KALLIOPE_API_PASSWORD`
* `KALLIOPE_PS_UIT_ENDPOINT`
* `KALLIOPE_PS_UIT_CONFIRMATION_ENDPOINT`
* `KALLIOPE_PS_IN_ENDPOINT`
* `INZENDING_BASE_URL`
* `EREDIENSTEN_BASE_URL`
* `BERICHTEN_CRON_PATTERN`: Pattern of the cron job that polls the API for berichten
* `BERICHTEN_IN_CONFIRMATION_CRON_PATTERN`: Pattern of the cron job that sends confirmations
* `INZENDINGEN_CRON_PATTERN`: Pattern of the cron job that sends inzendingen

Optional environment variables:

* `MU_APPLICATION_GRAPH`
* `MU_SPARQL_ENDPOINT`
* `MU_SPARQL_UPDATEPOINT`
* `MAX_MESSAGE_AGE`: Max age of the messages requested to the API (in days), _default: 3_. This value could theoretically be equal to that of `RUN_INTERVAL`, but a margin is advised to take eventual application or API downtime into account (to not miss any older messages).
* `MAX_SENDING_ATTEMPTS`: How many times the service can attempt to send out a certain message, _default: 3_. Prevents the API from getting the same request (that it won't accept) over and over again.
* `MAX_CONFIRMATION_ATTEMPTS`: How many times the service can attempt to send out a confirmation for a certain message, _default: 20_.

## Usage

Note that this service relies on the message-property `schema:dateReceived` not being set for finding messages that still need to be sent via the Kalliope API.

When an error is encoutered by the service, it will generate a [KalliopeSyncError](https://github.com/lblod/sync-with-kalliope-error-notification-service#kalliope-sync-error) that will be then processed and sent as an email.

## Develoment

### Set up the stack

```
berichtencentrum-sync-with-kalliope:
  build: /path/to/sources/berichtencentrum-sync-with-kalliope-service/
  volumes:
    - /path/to/sources/Loket/berichtencentrum-sync-with-kalliope-service/:/app/
    - /path/to/sources/berichtencentrum-sync-with-kalliope-service/data/files/:/data/files/
```

### Test

To retrieve poststukken to be able to test, this command can be helpful :

```
curl -u username:password --insecure 'http://<ip-address>:8090/api/poststuk-uit?vanaf=2020-05-01T00%3A00%3A00%2B02%3A00&aantal=10'
```
