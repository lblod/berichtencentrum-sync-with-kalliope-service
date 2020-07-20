import os
from pytz import timezone
from datetime import datetime
from helpers import log
from sudo_query_helpers import query, update
from kalliope_adapter import post_kalliope_inzending_in
from kalliope_adapter import open_kalliope_api_session
from queries import construct_unsent_inzendingen_query
from queries import construct_increment_inzending_attempts_query
from queries import construct_inzending_sent_query
from queries import construct_create_kalliope_sync_error_query
from update_with_supressed_fail import update_with_suppressed_fail


TIMEZONE = timezone('Europe/Brussels')
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))
INZENDING_BASE_URL = os.environ.get('INZENDING_BASE_URL')


def process_inzendingen():
    """
    Fetch submissions that have to be sent to Kalliope from the triple store,
        convert them to the correct format for the Kalliope API, post them and finally mark them as sent.
    :returns: None
    """
    q = construct_unsent_inzendingen_query(MAX_SENDING_ATTEMPTS)
    inzendingen = query(q)['results']['bindings']
    inzendingen = [parse_inzending_sparql_response(inzending_res) for inzending_res in inzendingen]
    log("Found {} submissions that need to be sent to the Kalliope API".format(len(inzendingen)))
    if len(inzendingen) == 0:
        return

    with open_kalliope_api_session() as session:
        for inzending in inzendingen:
            try:
                #  NOTE: Add graph as argument to query because Virtuoso
                bestuurseenheid_uuid = inzending['afzenderUri'].split('/')[-1]
                graph = \
                    "http://mu.semte.ch/graphs/organizations/{}/LoketLB-toezichtGebruiker".format(bestuurseenheid_uuid)
                try:
                    post_result = post_kalliope_inzending_in(INZENDING_IN_PATH, session, inzending)
                except Exception as e:
                    message = """
                              Something went wrong while posting following inzending in, skipping: {}\n{}
                              """.format(inzending, e)

                    error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, inzending['uri'], message, e)
                    update(error_query)
                    attempt_query = construct_increment_inzending_attempts_query(graph, inzending['uri'])
                    update(attempt_query)
                    log(message)

                    continue

                if post_result:
                    #  We consider the moment when the api-call succeeded the 'ontvangen'-time
                    ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat()
                    q_sent = construct_inzending_sent_query(graph, inzending['uri'], ontvangen)
                    update(q_sent)
                    log("successfully sent submission {} to Kalliope".format(inzending['uri']))

            except Exception as e:
                inzending_uri = inzending.get('uri')
                message = """
                           General error while trying to process inzending {}.
                            Error: {}
                          """.format(inzending_uri, e)
                error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, inzending_uri, message, e)
                update_with_suppressed_fail(error_query)
                # TODO: graph here should be re-thought...
                # attempt_query = construct_increment_inzending_attempts_query(graph, inzending_uri)
                # update_with_suppressed_fail(attempt_query)
                log(message)
    pass


def parse_inzending_sparql_response(inzending_res):
    inzending = {
        'uri': inzending_res['inzending']['value'],
        'afzenderUri': inzending_res['bestuurseenheid']['value'],
        'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' +
        inzending_res.get('sessionDate', {}).get('value', '').split('T')[0],
        'inhoud': INZENDING_BASE_URL + '/' + inzending_res['inzendingUuid']['value'],
        'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
        'typeMelding': inzending_res['decisionType']['value'],
    }
    return inzending
