import os
from datetime import datetime, timedelta
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import parse_kalliope_poststuk_uit
from .kalliope_adapter import  construct_kalliope_poststuk_in
from .kalliope_adapter import  get_kalliope_poststukken_uit
from .kalliope_adapter import  post_kalliope_poststuk_in
from .queries import construct_bericht_exists_query
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_conversatie_query
from .queries import construct_insert_bericht_query
from .queries import construct_unsent_berichten_query
from .queries import construct_bericht_sent_query
from .queries import construct_update_last_bericht_query_part1
from .queries import construct_update_last_bericht_query_part2

ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = "https://kalliope-svc-test.abb.vlaanderen.be/glapi/poststuk-uit"

def process_berichten_in():
    """
    Fetch Berichten from the Kalliope-api, parse them, and if needed, import them into the triple store.

    :returns:
    """
    MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE')) #in days
    vanaf = datetime.now() - timedelta(days=MAX_MESSAGE_AGE)
    tot = datetime.now()
    log("Pulling poststukken from kalliope API for period {} - {}".format(vanaf, tot))
    api_query_params = {
        'vanaf': vanaf.isoformat(),
        'tot': tot.isoformat(),
        'dossierTypes': "https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/klacht",
        'aantal': str(1000)
    }
    api_auth = (os.environ.get('KALLIOPE_API_USERNAME'), os.environ.get('KALLIOPE_API_PASSWORD'))
    try:
        poststukken = get_kalliope_poststukken_uit(PS_UIT_PATH, api_auth, api_query_params)
        log('Retrieved {} poststukken uit from Kalliope'.format(len(poststukken)))
    except Exception as e:
        log("Something went wrong while accessing the Kalliope API. Aborting: {}".format(e))
        return
    for poststuk in poststukken:
        try:
            (conversatie, bericht) = parse_kalliope_poststuk_uit(poststuk)
        except Exception as e:
            log("Something went wrong parsing following poststuk uit, skipping: {}\n{}".format(poststuk, e))
            continue
        bestuurseenheid_uuid = bericht['naar'].split('/')[-1]
        graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
        q = construct_bericht_exists_query(graph, bericht['uri'])
        query_result = query(q)['results']['bindings']
        if not query_result: #Bericht is not in our DB yet. We should insert it.
            log("Bericht '{}' - {} is not in DB yet.".format(conversatie['betreft'], bericht['verzonden']))
            q2 = construct_conversatie_exists_query(graph, conversatie['dossiernummer'])
            query_result2 = query(q2)['results']['bindings']
            if query_result2: #conversatie to which the bericht is linked, exists.
                conversatie_uri = query_result2[0]['conversatie']['value']
                log("Existing conversation '{}' inserting new message sent @ {}".format(conversatie['betreft'], bericht['verzonden']))
                q_bericht = construct_insert_bericht_query(graph, bericht, conversatie_uri)
                result = update(q_bericht)
            else: #conversatie to which the bericht is linked does not exist yet.
                log("Non-existing conversation '{}' inserting new conversation + message sent @ {}".format(conversatie['betreft'], bericht['verzonden']))
                conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
                q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht)
                result = update(q_conversatie)
            # Updating ext:lastMessage link for each conversation (in 2 parts because Virtuoso)
            update(construct_update_last_bericht_query_part1())
            update(construct_update_last_bericht_query_part2())
        else: #bericht already exists in our DB
            log("Bericht '{}' - {} already exists in our DB, skipping ...".format(conversatie['betreft'], bericht['verzonden']))
            pass
    
def process_berichten_out(arg):
    """
    Fetch Berichten that have to be sent from the triple store, convert them to the correct format for the Kalliope API, post them and finally mark them as sent.

    :param ?:
    :returns:
    """
    q = construct_unsent_berichten_query(PUBLIC_GRAPH, ABB_URI)
    berichten = query(q)['results']['bindings']
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
