import os
from pytz import timezone
from datetime import datetime

import requests.exceptions

from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import construct_kalliope_poststuk_in
from .kalliope_adapter import open_kalliope_api_session
from .kalliope_adapter import post_kalliope_poststuk_in
from .queries import construct_unsent_berichten_query
from .queries import construct_select_bijlagen_query
from .queries import construct_increment_bericht_attempts_query
from .queries import construct_bericht_sent_query
from .queries import construct_select_original_bericht_query
from .queries import construct_create_kalliope_sync_error_query
from .update_with_supressed_fail import update_with_suppressed_fail


TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))
INZENDING_BASE_URL = os.environ.get('INZENDING_BASE_URL')
PS_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')


def process_berichten_out():
    """
    Fetch Berichten that have to be sent to Kalliope from the triple store,
    convert them to the correct format for the Kalliope API, post them and finally mark them as sent.

    :returns: None
    """
    q = construct_unsent_berichten_query(ABB_URI, MAX_SENDING_ATTEMPTS)
    berichten = query(q)['results']['bindings']
    log("Found {} berichten that need to be sent to the Kalliope API".format(len(berichten)))
    if len(berichten) == 0:
        return
    with open_kalliope_api_session() as session:
        for bericht_res in berichten:
            try:
                (bericht, conversatie, bijlagen) = prepare_message_and_conversation(bericht_res)
                poststuk_in = construct_kalliope_poststuk_in(conversatie, bericht)
                bestuurseenheid_uuid =\
                    bericht['van'].split('/')[-1]  # NOTE: Add graph as argument to query because Virtuoso
                graph =\
                    "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
                log("Posting bericht <{}>. Payload: {}".format(bericht['uri'], poststuk_in))

                post_result = send_message(session, poststuk_in, bericht, bijlagen, graph)
                if post_result:
                    set_message_as_sent(bericht, bijlagen, graph)

            except Exception as e:
                message = """
                            General error while trying to send bericht {}.
                            Error: {}
                          """.format(bericht['uri'] if 'bericht' in locals() else "[No message defined]", e)
                error_query =\
                    construct_create_kalliope_sync_error_query(PUBLIC_GRAPH,
                                                               bericht['uri'] if 'bericht' in locals() else None,
                                                               message,
                                                               e)
                update_with_suppressed_fail(error_query)
                log(message)
    pass


def prepare_message_and_conversation(bericht_res):
    bericht = {
        'uri': bericht_res['bericht']['value'],
        'van': bericht_res['van']['value'],
        'verzonden': bericht_res['verzonden']['value'],
        'inhoud': bericht_res['inhoud']['value'],
    }

    origineel_bericht_uri = get_initial_message_uri(bericht)

    REPLY_SUBJECT_PREFIX = "Reactie op "
    betreft = REPLY_SUBJECT_PREFIX + bericht_res['betreft']['value']
    conversatie = {
        'inhoud': bericht_res['inhoud']['value'],
        'betreft': betreft,
        'origineelBerichtUri': origineel_bericht_uri
    }
    if 'dossieruri' in bericht_res:
        conversatie['dossierUri'] = bericht_res['dossieruri']['value']
    q_bijlagen = construct_select_bijlagen_query(PUBLIC_GRAPH, bericht['uri'])  # TEMP: bijlage in public graph
    bijlagen = query(q_bijlagen)['results']['bindings']
    bericht['bijlagen'] = []
    for bijlage_res in bijlagen:
        bijlage = {
            'name': bijlage_res['bijlagenaam']['value'],
            'filepath': bijlage_res['file']['value'].replace("share://", "", 1),
            'type': bijlage_res['type']['value'],
        }
        bericht['bijlagen'].append(bijlage)

    return bericht, conversatie, bijlagen


def get_initial_message_uri(bericht):
    q_origineel = construct_select_original_bericht_query(bericht['uri'])
    return query(q_origineel)['results']['bindings'][0]['origineelbericht']['value']


def send_message(session, poststuk_in, bericht, bijlagen, graph):
    try:
        return post_kalliope_poststuk_in(PS_IN_PATH, session, poststuk_in)
    except Exception as e:
        message = "Something went wrong while posting following poststuk in, skipping: {}\n{}".format(poststuk_in,
                                                                                                      e)
        update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, bericht['uri'], message, e))
        update(construct_increment_bericht_attempts_query(graph, bericht['uri']))
        log(message)
        raise e


def set_message_as_sent(bericht, bijlagen, graph):
    # We consider the moment when the api-call succeeded the 'ontvangen'-time
    ontvangen = datetime.now(tz=TIMEZONE).\
                                 replace(microsecond=0).isoformat()
    q_sent = construct_bericht_sent_query(graph, bericht['uri'], ontvangen)
    update(q_sent)
    log("successfully sent bericht {} with {} bijlagen to Kalliope".format(bericht['uri'],
                                                                           len(bijlagen)))
