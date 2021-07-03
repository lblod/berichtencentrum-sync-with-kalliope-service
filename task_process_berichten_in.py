import os
from datetime import datetime, timedelta
from pytz import timezone

import requests.exceptions

import helpers
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import parse_kalliope_poststuk_uit
from .kalliope_adapter import parse_kalliope_bijlage
from .kalliope_adapter import open_kalliope_api_session
from .kalliope_adapter import get_kalliope_poststukken_uit
from .kalliope_adapter import BIJLAGEN_FOLDER_PATH
from .queries import construct_bestuurseenheid_exists_query
from .queries import construct_bericht_exists_query
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_bijlage_query
from .queries import construct_insert_conversatie_query
from .queries import construct_insert_bericht_query
from .queries import construct_update_conversatie_type_query
from .queries import construct_update_last_bericht_query
from .queries import construct_create_kalliope_sync_error_query
from .queries import construct_dossierbehandelaar_exists_query
from .queries import construct_insert_dossierbehandelaar_query
from .queries import construct_link_dossierbehandelaar_query
from .update_with_supressed_fail import update_with_suppressed_fail

TIMEZONE = timezone('Europe/Brussels')
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = os.environ.get('KALLIOPE_PS_UIT_ENDPOINT')
MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE'))  # in days

class UnknownBestuurseenheidError(Exception):
    """Raised when the bestuurseenheid we received in unknown in our system."""
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

                bestuurseeheid_uri = bericht['naar']
                bestuurseenheid_uuid = bestuurseeheid_uri.split('/')[-1]
                bestuurseenheid_in_db = is_bestuurseenheid_in_db(bestuurseeheid_uri)

                if not bestuurseenheid_in_db:
                    message = "Bestuurseenheid with uri {} not found in our database".format(bestuurseeheid_uri)
                    log(message)
                    raise UnknownBestuurseenheidError(message)

                else:
                    log("Bestuurseeneheid {} found, proceeding with processing the message".format(bestuurseeheid_uri))
                    graph =\
                        "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
                    message_in_db = is_message_in_db(bericht, graph)

                    if not message_in_db:  # Bericht is not in our DB yet. We should insert it.
                        log("Bericht '{}' - {} is not in DB yet.".format(conversatie['betreft'], bericht['verzonden']))
                        insert_message_in_db(conversatie, bericht, poststuk, session, graph)

                    else:  # bericht already exists in our DB
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


def is_bestuurseenheid_in_db(bestuurseeheid_uri):
    q = construct_bestuurseenheid_exists_query(bestuurseeheid_uri)
    query_result = query(q)['boolean']
    return query_result


def is_message_in_db(bericht, graph):
    q = construct_bericht_exists_query(graph, bericht['uri'])
    query_result = query(q)['results']['bindings']
    return False if not query_result else True


def insert_message_in_db(conversatie, bericht, poststuk, session, graph):
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

    delivery_timestamp = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat()

    q2 = construct_conversatie_exists_query(graph, conversatie['referentieABB'])
    query_result2 = query(q2)['results']['bindings']
    if query_result2:  # The conversatie to which the bericht is linked exists.
        conversatie['uri'] = query_result2[0]['conversatie']['value']

        log("Existing conversation '{}' inserting new message sent @ {}".format(conversatie['betreft'],
                                                                                bericht['verzonden']))

        q_bericht = construct_insert_bericht_query(graph, bericht, conversatie['uri'], delivery_timestamp)

        try:
            update(q_bericht)
            q_type_communicatie =\
                construct_update_conversatie_type_query(graph, conversatie['uri'], bericht['type_communicatie'])
            update(q_type_communicatie)
            # TODO: perhaps later first save bijlagen and the meta-data
            save_bijlagen(graph, PUBLIC_GRAPH, bericht, bericht['bijlagen'])
        except Exception as e:
            message = "Something went wrong inserting new message or conversation"
            update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
            log("{}, skipping: {}\n{}".format(message, poststuk, e))
            raise e

    else:  # The conversatie to which the bericht is linked does not exist yet.
        log("Non-existing conversation '{}' inserting new conversation + message sent @ {}".
            format(conversatie['betreft'], bericht['verzonden']))

        conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
        q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht, delivery_timestamp)
        try:
            update(q_conversatie)
            save_bijlagen(graph, PUBLIC_GRAPH, bericht, bericht['bijlagen'])
        except Exception as e:
            message = "Something went wrong inserting new message"
            update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
            log("{}, skipping: {}\n{}".format(message, poststuk, e))
            raise e

    try:
        update(construct_update_last_bericht_query(conversatie['uri']))
        insert_dossierbehandelaar_in_db(graph, bericht)
    except Exception as e:
        message = "Something went wrong updating conversation and dossierbehandelaar"
        update(construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, poststuk['uri'], message, e))
        log("{}, skipping: {}\n{}".format(message, poststuk, e))
        raise e

def save_bijlagen(bericht_graph_uri, file_graph, bericht, bijlagen):
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
        q_bijlage = construct_insert_bijlage_query(bericht_graph_uri,
                                                   file_graph,
                                                   bericht['uri'],
                                                   bijlage,
                                                   file)  # TEMP: bijlage in public graph
        update(q_bijlage)


def insert_dossierbehandelaar_in_db(graph, bericht):
    q_dossierbehandelaar_exists = construct_dossierbehandelaar_exists_query(graph, bericht['dossierbehandelaar'])
    query_result_dossierbehandelaar_exists = query(q_dossierbehandelaar_exists)['results']['bindings']
    if not query_result_dossierbehandelaar_exists:
        q_dossierbehandelaar = construct_insert_dossierbehandelaar_query(graph, bericht)
        update(q_dossierbehandelaar)
    else:
        bericht['dossierbehandelaar']['uri'] = query_result_dossierbehandelaar_exists[0]['dossierbehandelaar']['value']

    q_link_dossierbehandelaar_bericht = construct_link_dossierbehandelaar_query(graph, bericht)
    update(q_link_dossierbehandelaar_bericht)
