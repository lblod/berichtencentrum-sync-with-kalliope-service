
required environment variables:

* `KALLIOPE_API_USERNAME`  
* `KALLIOPE_API_PASSWORD`


Optional environment variables:
* `MU_APPLICATION_GRAPH`
* `MU_SPARQL_ENDPOINT`
* `MU_SPARQL_UPDATEPOINT`

* `RUN_INTERVAL`: How frequent the service to poll the API must run (in minutes), _default: 5_
* `MAX_MESSAGE_AGE`: Max age of the messages requested to the API (in days), _default: 3_. This value could theoretically be equal to that of `RUN_INTERVAL`, but a margin is advised to take eventual application or API downtime into account (to not miss any older messages).
