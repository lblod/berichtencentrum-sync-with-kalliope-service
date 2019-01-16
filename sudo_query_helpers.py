import os
from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import log

sparqlQuery = SPARQLWrapper(os.environ.get('MU_SPARQL_ENDPOINT'), returnFormat=JSON)
sparqlQuery.addCustomHttpHeader('mu-auth-sudo', 'true')
sparqlUpdate = SPARQLWrapper(os.environ.get('MU_SPARQL_UPDATEPOINT'), returnFormat=JSON)
sparqlUpdate.method = 'POST'
sparqlUpdate.addCustomHttpHeader('mu-auth-sudo', 'true')

def query(the_query):
    """Execute the given SPARQL query (select/ask/construct)on the triple store and returns the results
    in the given returnFormat (JSON by default)."""
    log("execute query: \n" + the_query)
    sparqlQuery.setQuery(the_query)
    return sparqlQuery.query().convert()


def update(the_query):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens."""
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        log("execute query: \n" + the_query)
        sparqlUpdate.query()