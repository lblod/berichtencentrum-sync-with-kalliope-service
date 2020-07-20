import os
from pytz import timezone
from datetime import datetime
from helpers import log
from sudo_query_helpers import query, update
from kalliope_adapter import construct_kalliope_inzending_in
from kalliope_adapter import post_kalliope_inzending_in
from kalliope_adapter import open_kalliope_api_session
from queries import construct_unsent_inzendingen_query
from queries import construct_increment_inzending_attempts_query
from queries import construct_inzending_sent_query
from queries import construct_create_kalliope_sync_error_query
from update_with_supressed_fail import update_with_suppressed_fail


TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = os.environ.get('KALLIOPE_PS_UIT_ENDPOINT')
PS_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE'))  # in days
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
    log("Found {} submissions that need to be sent to the Kalliope API".format(len(inzendingen)))
    if len(inzendingen) == 0:
        return

    with open_kalliope_api_session() as session:
        for inzending_res in inzendingen:
            try:
                inzending = {
                    'uri': inzending_res['inzending']['value'],
                    'afzenderUri': inzending_res['bestuurseenheid']['value'],
                    'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' +
                    inzending_res.get('sessionDate', {}).get('value', '').split('T')[0],
                    'inhoud': INZENDING_BASE_URL + '/' + inzending_res['inzendingUuid']['value'],
                    'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
                    'typeMelding': inzending_res['decisionType']['value'],
                }

                inzending_in = construct_kalliope_inzending_in(inzending)

                #  NOTE: Add graph as argument to query because Virtuoso
                bestuurseenheid_uuid = inzending['afzenderUri'].split('/')[-1]
                graph = \
                    "http://mu.semte.ch/graphs/organizations/{}/LoketLB-toezichtGebruiker".format(bestuurseenheid_uuid)
                log("Posting inzending <{}>. Payload: {}".format(inzending['uri'], inzending_in))

                try:
                    post_result = post_kalliope_inzending_in(INZENDING_IN_PATH, session, inzending_in)
                except Exception as e:
                    message = """
                              Something went wrong while posting following inzending in, skipping: {}\n{}
                              """.format(inzending_in, e)

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
                inzending_uri = inzending_res.get('inzending', {}).get('value')
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