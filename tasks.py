import os
from datetime import datetime, timedelta
from pytz import timezone

import requests.exceptions

import helpers
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import parse_kalliope_poststuk_uit
from .kalliope_adapter import parse_kalliope_bijlage
from .kalliope_adapter import construct_kalliope_poststuk_in
from .kalliope_adapter import open_kalliope_api_session
from .kalliope_adapter import get_kalliope_poststukken_uit
from .kalliope_adapter import post_kalliope_poststuk_in
from .kalliope_adapter import construct_kalliope_inzending_in
from .kalliope_adapter import post_kalliope_inzending_in
from .queries import construct_bericht_exists_query
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_bijlage_query
from .queries import construct_insert_conversatie_query
from .queries import construct_insert_bericht_query
from .queries import construct_unsent_berichten_query
from .queries import construct_update_conversatie_type_query
from .queries import construct_select_bijlagen_query
from .queries import construct_increment_bericht_attempts_query
from .queries import construct_bericht_sent_query
from .queries import construct_update_last_bericht_query_part1
from .queries import construct_update_last_bericht_query_part2
from .queries import construct_select_original_bericht_query
from .queries import construct_unsent_inzendingen_query
from .queries import construct_increment_inzending_attempts_query
from .queries import construct_inzending_sent_query
from .queries import construct_create_kalliope_sync_error_query
from .kalliope_adapter import BIJLAGEN_FOLDER_PATH

TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = os.environ.get('KALLIOPE_PS_UIT_ENDPOINT')
PS_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE'))  # in days
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))
INZENDING_BASE_URL = os.environ.get('INZENDING_BASE_URL')


def update_with_suppressed_fail(query_string):
    try:
        update(query_string)
    except Exception as e:
        log("""
              WARNING: an error occured during the update_with_suppressed_fail.
                       Message {}
                       Query {}
            """.format(e, query_string))
        log("""WARNING: I am sorry you have to read this message""")


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
                post_result = send_inzending(inzending_res, session)
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
                continue
    pass


def process_berichten_in():
    """
    Fetch Berichten from the Kalliope-api, parse them, and if needed, import them into the triple store.

    :returns: None
    """
    vanaf = datetime.now(tz=TIMEZONE) - timedelta(days=MAX_MESSAGE_AGE)
    log("Pulling poststukken from kalliope API for period {} - now".format(vanaf.isoformat()))
    with open_kalliope_api_session() as session:
        try:
            poststukken = get_kalliope_poststukken_uit(PS_UIT_PATH, session, vanaf)
            log('Retrieved {} poststukken uit from Kalliope'.format(len(poststukken)))
        except requests.exceptions.RequestException as e:
            message = "Something went wrong while accessing the Kalliope API. Aborting: {}".format(e)
            update_with_suppressed_fail(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, None, message, e))
            log(message)
            return

        for poststuk in poststukken:
            try:
                (conversatie, bericht) = parse_kalliope_poststuk_uit(poststuk, session)

                bestuurseenheid_uuid = bericht['naar'].split('/')[-1]
                graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
                message_in_db = is_message_in_db(bericht, graph)

                if not message_in_db: #Bericht is not in our DB yet. We should insert it.
                    insert_message_in_db(conversatie, bericht, poststuk, session, graph)
                else: #bericht already exists in our DB
                    log("Bericht '{}' - {} already exists in our DB, skipping ...".format(conversatie['betreft'],
                                                                                        bericht['verzonden']))

            except Exception as e:
                message = """
                        General error while trying to process message {}.
                            Error: {}
                        """.format(poststuk['uri'], e)
                error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e)
                update_with_suppressed_fail(error_query)
                log(message)


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
                bestuurseenheid_uuid = bericht['van'].split('/')[-1] # NOTE: Add graph as argument to query because Virtuoso
                graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
                log("Posting bericht <{}>. Payload: {}".format(bericht['uri'], poststuk_in))

                send_message(session, poststuk_in, bericht, bijlagen, graph)
            except Exception as e:
                message = """
                            General error while trying to send bericht {}.
                            Error: {}
                          """.format(bericht['uri'] if 'bericht' in locals() else "[No message defined]", e)
                error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, bericht['uri'] if 'bericht' in locals() else None, message, e)
                update_with_suppressed_fail(error_query)
                log(message)
    pass


### PRIVATE ###

def save_bijlagen(bijlagen):
    for bijlage in bijlagen:
        bijlage['uri'] = "http://mu.semte.ch/services/file-service/files/{}".format(bijlage['id'])
        file = {
            'id': bijlage['id'],
            'uuid': helpers.generate_uuid(),
            'name': bijlage['id'] + "." + bijlage['extension'],
            'uri': "share://" + bijlage['id'] + "." + bijlage['extension'],
        }
        filepath = os.path.join(BIJLAGEN_FOLDER_PATH, file['name'])
        f = open(filepath, 'wb')
        f.write(bijlage['buffer'])
        q_bijlage = construct_insert_bijlage_query(graph,
                                                PUBLIC_GRAPH,
                                                bericht['uri'],
                                                bijlage,
                                                file) # TEMP: bijlage in public graph
        result = update(q_bijlage)


def construct_inzending_in(inzending_res):
    inzending = {
        'uri': inzending_res['inzending']['value'],
        'afzenderUri': inzending_res['bestuurseenheid']['value'],
        'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' +
        inzending_res.get('sessionDate', {}).get('value', '').split('T')[0],
        'inhoud': INZENDING_BASE_URL + '/' + inzending_res['inzendingUuid']['value'],
        'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
        'typeMelding': inzending_res['decisionType']['value'],
    }
    return inzending, construct_kalliope_inzending_in(inzending)


def send_inzending(inzending_res, session):
    (inzending, inzending_in) = construct_inzending_in(inzending_res)
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
        raise e


def parse_poststuk_uit(poststuk, session):
    try:
        return parse_kalliope_poststuk_uit(poststuk, session)
    except Exception as e:
        message = "Something went wrong parsing poststuk uit"
        update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
        log("{}, skipping: {}\n{}".format(message, poststuk, e))
        raise e


def is_message_in_db(bericht, graph):
    q = construct_bericht_exists_query(graph, bericht['uri'])
    query_result = query(q)['results']['bindings']
    return False if not query_result else True


def insert_message_in_db(conversatie, bericht, poststuk, session, graph):
    log("Bericht '{}' - {} is not in DB yet.".format(conversatie['betreft'],
                                                    bericht['verzonden']))
    # Fetch attachments & parse
    bericht['bijlagen'] = []
    try:
        for ps_bijlage in bericht['bijlagen_refs']:
            bijlage = parse_kalliope_bijlage(ps_bijlage, session)
            bericht['bijlagen'].append(bijlage)
    except Exception as e:
        message = "Something went wrong while parsing a bijlage for bericht {} sent @ {}".format(conversatie['betreft'],
                                                                                                bericht['verzonden'])
        update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
        helpers.log(message)
        raise e

    q2 = construct_conversatie_exists_query(graph, conversatie['dossiernummer'])
    query_result2 = query(q2)['results']['bindings']
    if query_result2: # The conversatie to which the bericht is linked exists.
        conversatie_uri = query_result2[0]['conversatie']['value']
        log("Existing conversation '{}' inserting new message sent @ {}".format(conversatie['betreft'],
                                                                                bericht['verzonden']))
        q_bericht = construct_insert_bericht_query(graph, bericht, conversatie_uri)
        try:
            result = update(q_bericht)
            q_type_communicatie = construct_update_conversatie_type_query(graph, conversatie_uri, bericht['type_communicatie'])
            result = update(q_type_communicatie)
            save_bijlagen(bericht['bijlagen'])
        except Exception as e:
            message = "Something went wrong inserting new message or conversation"
            update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
            log("{}, skipping: {}\n{}".format(message, poststuk, e))
            raise e

    else: # The conversatie to which the bericht is linked does not exist yet.
        log("Non-existing conversation '{}' inserting new conversation + message sent @ {}".format(conversatie['betreft'],
                                                                                                bericht['verzonden']))
        conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
        q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht)
        try:
            result = update(q_conversatie)
            save_bijlagen(bericht['bijlagen'])
        except Exception as e:
            message = "Something went wrong inserting new message"
            update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
            log("{}, skipping: {}\n{}".format(message, poststuk, e))
            raise e

    # Updating ext:lastMessage link for each conversation (in 2 parts because Virtuoso)
    update(construct_update_last_bericht_query_part1())
    update(construct_update_last_bericht_query_part2())


def send_message(session, poststuk_in, bericht, bijlagen, graph):
    try:
        post_result = post_kalliope_poststuk_in(PS_IN_PATH, session, poststuk_in)
    except requests.exceptions.RequestException as e:
        message = "Something went wrong while posting following poststuk in, skipping: {}\n{}".format(poststuk_in,
                                                                                                      e)
        update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, bericht['uri'], message, e))
        update(construct_increment_bericht_attempts_query(graph, bericht['uri']))
        log(message)
        raise e
    if post_result:
        set_message_as_sent(bericht, bijlagen, graph)

def set_message_as_sent(bericht, bijlagen, graph):
    ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat() # We consider the moment when the api-call succeeded the 'ontvangen'-time
    q_sent = construct_bericht_sent_query(graph, bericht['uri'], ontvangen)
    update(q_sent)
    log("successfully sent bericht {} with {} bijlagen to Kalliope".format(bericht['uri'],
                                                                           len(bijlagen)))


def get_initial_message_uri(bericht):
    q_origineel = construct_select_original_bericht_query(bericht['uri'])
    return query(q_origineel)['results']['bindings'][0]['origineelbericht']['value']


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
        'dossiernummer': bericht_res['dossiernummer']['value'],
        'betreft': betreft,
        'origineelBerichtUri': origineel_bericht_uri
    }
    if 'dossieruri' in bericht_res:
        conversatie['dossierUri'] = bericht_res['dossieruri']['value']
    q_bijlagen = construct_select_bijlagen_query(PUBLIC_GRAPH, bericht['uri']) # TEMP: bijlage in public graph
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


### MOCK DATA FOR TESTING PURPOSE ###

## process_inzendingen
#inzendingen = [
#    {
#        'inzending': {
#            'value': 'http://data.lblod.info/submissions/2bc74060-9149-11ea-9b69-8543f48a35b0'
#        },
#        'bestuurseenheid': {
#            'value': 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'
#        },
#        'decisionTypeLabel': {
#            'value': 'Budget'
#        },
#        'sessionDate': {
#            'value': '2020-05-05T11:32:52.976Z'
#        },
#        'inzendingUuid': {
#            'value': '2bc74060-9149-11ea-9b69-8543f48a35b0'
#        },
#        'decisionType': {
#            'value': 'https://data.vlaanderen.be/id/concept/BesluitType/40831a2c-771d-4b41-9720-0399998f1873'
#        }
#    },
#    {
#        'inzending': {
#            'value': 'http://data.lblod.info/submissions/3bc74060-9149-11ea-9b69-8543f48a35b0'
#        },
#        'bestuurseenheid': {
#            'value': 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'
#        },
#        'decisionTypeLabel': {
#            'value': 'Budget'
#        },
#        'sessionDate': {
#            'value': '2020-06-06T11:32:52.976Z'
#        },
#        'inzendingUuid': {
#            'value': '3bc74060-9149-11ea-9b69-8543f48a35b0'
#        },
#        'decisionType': {
#            'value': 'https://data.vlaanderen.be/id/concept/BesluitType/40831a2c-771d-4b41-9720-0399998f1873'
#        }
#    }
#]


## process_berichten_in
#poststukken = [
#    {
#        "uri":"http://abb.groundlion.be/#!/case/detail/f537e1e3-3a1e-4685-84f3-94ad8b51e990/6f2c4bea-dc7e-4115-9ad6-e5ad0345e445",
#        "naam":"POST_UIT2020.012159",
#        "dossier": {
#            "uri":"http://abb.groundlion.be/#!/case/detail/e9285646-1226-409c-90b3-31384e90d779/b7df106e-203d-4f8b-b9e0-8f5e3669c0f7",
#            "naam":"POST_BOR2020.0612",
#            "dossierType":"type"
#        },
#        "bestemmeling": {
#            "uri":"http://data.lblod.info/id/bestuurseenheden/43d5a7c66986ee2e88090b3988ea0179ad4abf5bbfb8864fc44012aa181d0e4d",
#            "naam":"BOOM"
#        },
#        "betreft":"Test 321",
#        "inhoud":"test extra info",
#        "bijlages":[
#            {
#                "url":"http://159.69.193.68:8090/api/bijlage/86d70e66-c394-439c-9d44-50823f3b4d80",
#                "naam":"lorem-ipsum.pdf",
#                "mimeType":"application/pdf"
#            }
#        ],
#        "creatieDatum":"2020-05-05T11:32:52.976Z",
#        "verzendDatum":"2020-05-05",
#        "datumBeschikbaar":"2020-05-05T11:32:56.1Z",
#        "typeCommunicatie":"Kennisgeving toezichtsbeslissing",
#        "dossierType":"https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit",
#        "dossierNummer":""
#    },
#    {
#        "uri":"http://abb.groundlion.be/#!/case/detail/g537e1e3-3a1e-4685-84f3-94ad8b51e990/6f2c4bea-dc7e-4115-9ad6-e5ad0345e445",
#        "naam":"POST_UIT2020.012160",
#        "dossier": {
#            "uri":"http://abb.groundlion.be/#!/case/detail/g9285646-1226-409c-90b3-31384e90d779/b7df106e-203d-4f8b-b9e0-8f5e3669c0f7",
#            "naam":"POST_BOR2020.0613",
#            "dossierType":"type"
#        },
#        "bestemmeling": {
#            "uri":"http://data.lblod.info/id/bestuurseenheden/43d5a7c66986ee2e88090b3988ea0179ad4abf5bbfb8864fc44012aa181d0e4d",
#            "naam":"BAM"
#        },
#        "betreft":"Test 432",
#        "inhoud":"test extra info",
#        "bijlages":[
#            {
#                "url":"http://159.69.193.68:8090/api/bijlage/g6d70e66-c394-439c-9d44-50823f3b4d80",
#                "naam":"lorem-ipsum.pdf",
#                "mimeType":"application/pdf"
#            }
#        ],
#        "creatieDatum":"2020-06-06T11:32:52.976Z",
#        "verzendDatum":"2020-06-06",
#        "datumBeschikbaar":"2020-06-06T11:32:56.1Z",
#        "typeCommunicatie":"Kennisgeving toezichtsbeslissing",
#        "dossierType":"https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit",
#        "dossierNummer":""
#    }
#]


# process_berichten_out
#berichten = [
#    {
#        'bericht': {
#            'value': 'http://data.lblod.info/id/berichten/5E2848B0A3ACB60008000424'
#        },
#        'van': {
#            'value': 'http://data.lblod.info/id/bestuurseenheden/6377f53f54990033c90de6101e263f4d9e41eb7c3e4f70dec48caccefc253760'
#        },
#        'verzonden': {
#            'value': '2020-01-22T13:05:52.429Z'
#        },
#        'inhoud': {
#            'value': ''
#        },
#        'betreft': {
#            'value': 'KLACHT2019.001102 tegen BLANKENBERGE: opvraging aan bestuur'
#        },
#        'dossiernummer': {
#            'value': 'KLACHT2019.001102'
#        },
#        'dossieruri': {
#            'value': 'https://kalliope.abb.vlaanderen.be/#!/case/detail/bf17a0f2-210a-4fbb-936a-21b047d7fa66/502174f6-3a1b-4048-b791-c751f6a4a7d3'
#        }
#    },
#    {
#        'bericht': {
#            'value': 'http://data.lblod.info/id/berichten/5CB994F1D5BECA00090003BE'
#        },
#        'van': {
#            'value': 'http://data.lblod.info/id/bestuurseenheden/514bd3d6ba551d4b2a7e866c7dafd65e371acd75240dc92610590c5804c641df'
#        },
#        'verzonden': {
#            'value': '2019-04-19T09:29:21.239Z'
#        },
#        'inhoud': {
#            'value': ''
#        },
#        'betreft': {
#            'value': 'KLACHT2019.000250 tegen Gemeente Meerhout: opvraging aan bestuur'
#        },
#        'dossiernummer': {
#            'value': 'KLACHT2019.000250'
#        },
#        'dossieruri': {
#            'value': 'https://kalliope.abb.vlaanderen.be/#!/case/detail/5bee644c78bd01732c78415b/5cab50bc0a3fb11b60e32883'
#        }
#    }
#]