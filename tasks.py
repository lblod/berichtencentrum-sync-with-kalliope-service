from datetime import datetime
from .kalliope_adapter import parse_kalliope_poststuk_uit
from .kalliope_adapter import  construct_kalliope_poststuk_in
from .kalliope_adapter import  get_kalliope_poststukken_uit
from .kalliope_adapter import  post_kalliope_poststuk_in
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_conversatie_query
from .queries import construct_insert_bericht_query
from .queries import construct_unsent_berichten_query
from .queries import construct_bericht_sent_query

ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"

def process_berichten_in(arg):
    """
    Fetch Berichten from the Kalliope-api, parse them, and if needed, import them into the triple store.

    :param ?:
    :returns:
    """
    poststukken = get_kalliope_poststukken_uit(arg)
    for poststuk in poststukken:
        (conversatie, bericht) = parse_kalliope_poststuk_uit(poststuk)# TODO:
        q = construct_conversatie_exists_query(PUBLIC_GRAPH, conversatie['dossiernummer'])
        query_result = helpers.query(q)['results']['bindings']
        if query_result: #Existing conversatie, look for it.
            conversatie_uri = query_result[0]['conversatie']['value']
            q_bericht = construct_insert_bericht_query(PUBLIC_GRAPH, bericht, conversatie_uri)
            result = helpers.update(q_bericht)
        else: #Non-existing conversatie, create one.
            q_conversatie = construct_insert_conversatie_query(PUBLIC_GRAPH, conversatie, bericht)
            result = helpers.update(q_conversatie)
    pass

def process_berichten_out(arg):
    """
    Fetch Berichten that have to be sent from the triple store, convert them to the correct format for the Kalliope API, post them and finally mark them as sent.

    :param ?:
    :returns:
    """
    q = construct_unsent_berichten_query(PUBLIC_GRAPH, ABB_URI)
    berichten = helpers.query(q)['results']['bindings']
    for bericht in berichten:
        bericht_uri = bericht['bericht']['value']
        dossiernummer = bericht['dossiernummer']['value']
        van = bericht['van']['value']
        verzonden = bericht['verzonden']['value']
        inhoud = bericht['inhoud']['value']
        poststuk = construct_kalliope_poststuk_in(arg)
        post_result = post_kalliope_poststuk_in(poststuk)
        if post_result:
            ontvangen = datetime.now().isoformat() #We consider the moment when the api-call succeeded the 'ontvangen'-time
            construct_bericht_sent_query(PUBLIC_GRAPH, bericht_uri, ontvangen)
    pass
