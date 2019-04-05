import os
from datetime import datetime, timedelta
from pytz import timezone
import pytz

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
from .queries import construct_select_bijlagen_query
from .queries import construct_increment_bericht_attempts_query
from .queries import construct_bericht_sent_query
from .queries import construct_update_last_bericht_query_part1
from .queries import construct_update_last_bericht_query_part2
from .queries import construct_select_original_bericht_query
from .queries import construct_unsent_inzendingen_query
from .queries import construct_select_inzending_bijlagen_query
from .queries import construct_increment_inzending_attempts_query
from .queries import construct_inzending_sent_query
from .kalliope_adapter import BIJLAGEN_FOLDER_PATH

TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = os.environ.get('KALLIOPE_PS_UIT_ENDPOINT')
PS_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE')) #in days
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))

def process_inzendingen():
    """
    Fetch inzendingen that have to be sent to Kalliope from the triple store,
        convert them to the correct format for the Kalliope API, post them and finally mark them as sent.

    :returns: None
    """
    q = construct_unsent_inzendingen_query(MAX_SENDING_ATTEMPTS)
    inzendingen = query(q)['results']['bindings']
    log("Found {} inzendingen that need to be sent to the Kalliope API".format(len(inzendingen)))

    if len(inzendingen) == 0:
        return

    with open_kalliope_api_session() as session:
        log('Session params : {}'.format(session.params))
        for inzending_res in inzendingen:
            inzending = {
                'uri': inzending_res['inzending']['value'],
                'afzenderUri': inzending_res['bestuurseenheid']['value'],
                'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' + inzending_res['sessionDate']['value'],
                'inhoud': '',
                'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
                'typeMelding': inzending_res['decisionType']['value'],
            }

            q_bijlagen = construct_select_inzending_bijlagen_query(PUBLIC_GRAPH, inzending['uri'])
            bijlagen = query(q_bijlagen)['results']['bindings']
            inzending['bijlagen'] = []
            for bijlage_res in bijlagen:
                bijlage = {
                    'name': bijlage_res['bijlagenaam']['value'],
                    'filepath': bijlage_res['file']['value'].strip("share://"),
                    'type': bijlage_res['type']['value'],
                }
                inzending['bijlagen'].append(bijlage)

            inzending_in = construct_kalliope_inzending_in(inzending)

            bestuurseenheid_uuid = inzending['afzenderUri'].split('/')[-1] # NOTE: Add graph as argument to query because Virtuoso
            graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-toezichtGebruiker".format(bestuurseenheid_uuid)
            log("Posting inzending <{}>. Payload: {}".format(inzending['uri'], inzending_in))

            try:
                post_result = post_kalliope_inzending_in(INZENDING_IN_PATH, session, inzending_in)
            except requests.exceptions.RequestException as e:
                log("Something went wrong while posting following inzending in, skipping: {}\n{}".format(inzending_in,
                                                                                                   e))
                update(construct_increment_inzending_attempts_query(graph, inzending['uri']))
                continue

            if post_result:
                ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat() # We consider the moment when the api-call succeeded the 'ontvangen'-time
                q_sent = construct_inzending_sent_query(graph, inzending['uri'], ontvangen)
                update(q_sent)
                log("successfully sent inzending {} with {} bijlagen to Kalliope".format(inzending['uri'],
                                                                                       len(bijlagen)))
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
            log("Something went wrong while accessing the Kalliope API. Aborting: {}".format(e))
            return

        for poststuk in poststukken:
            try:
                (conversatie, bericht) = parse_kalliope_poststuk_uit(poststuk, session)
            except Exception as e:
                log("Something went wrong parsing following poststuk uit, skipping: {}\n{}".format(poststuk,
                                                                                                   e))
                continue
            bestuurseenheid_uuid = bericht['naar'].split('/')[-1]
            graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
            q = construct_bericht_exists_query(graph, bericht['uri'])
            query_result = query(q)['results']['bindings']
            if not query_result: #Bericht is not in our DB yet. We should insert it.
                log("Bericht '{}' - {} is not in DB yet.".format(conversatie['betreft'],
                                                                 bericht['verzonden']))
                # Fetch attachments & parse
                bericht['bijlagen'] = []
                for ps_bijlage in bericht['bijlagen_refs']:
                    try:
                        bijlage = parse_kalliope_bijlage(ps_bijlage, session)
                        bericht['bijlagen'].append(bijlage)
                    except Exception as e:
                        helpers.log("Something went wrong while parsing a bijlage for bericht {} sent @ {}".format(conversatie['betreft'],
                                                                                                                   bericht['verzonden']))
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
                q2 = construct_conversatie_exists_query(graph, conversatie['dossiernummer'])
                query_result2 = query(q2)['results']['bindings']
                if query_result2: #conversatie to which the bericht is linked, exists.
                    conversatie_uri = query_result2[0]['conversatie']['value']
                    log("Existing conversation '{}' inserting new message sent @ {}".format(conversatie['betreft'],
                                                                                            bericht['verzonden']))
                    q_bericht = construct_insert_bericht_query(graph, bericht, conversatie_uri)
                    result = update(q_bericht)
                    save_bijlagen(bericht['bijlagen'])
                else: #conversatie to which the bericht is linked does not exist yet.
                    log("Non-existing conversation '{}' inserting new conversation + message sent @ {}".format(conversatie['betreft'],
                                                                                                               bericht['verzonden']))
                    conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
                    q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht)
                    result = update(q_conversatie)
                    save_bijlagen(bericht['bijlagen'])
                # Updating ext:lastMessage link for each conversation (in 2 parts because Virtuoso)
                update(construct_update_last_bericht_query_part1())
                update(construct_update_last_bericht_query_part2())
            else: #bericht already exists in our DB
                log("Bericht '{}' - {} already exists in our DB, skipping ...".format(conversatie['betreft'],
                                                                                      bericht['verzonden']))
                pass

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
            bericht = {
                'uri': bericht_res['bericht']['value'],
                'van': bericht_res['van']['value'],
                'verzonden': bericht_res['verzonden']['value'],
                'inhoud': bericht_res['inhoud']['value'],
            }
            q_origineel = construct_select_original_bericht_query(bericht['uri'])
            origineel_bericht_uri = query(q_origineel)['results']['bindings'][0]['origineelbericht']['value']
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
                    'filepath': bijlage_res['file']['value'].strip("share://"),
                    'type': bijlage_res['type']['value'],
                }
                bericht['bijlagen'].append(bijlage)

            poststuk_in = construct_kalliope_poststuk_in(conversatie, bericht)
            bestuurseenheid_uuid = bericht['van'].split('/')[-1] # NOTE: Add graph as argument to query because Virtuoso
            graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
            log("Posting bericht <{}>. Payload: {}".format(bericht['uri'], poststuk_in))
            try:
                post_result = post_kalliope_poststuk_in(PS_IN_PATH, session, poststuk_in)
            except requests.exceptions.RequestException as e:
                log("Something went wrong while posting following poststuk in, skipping: {}\n{}".format(poststuk_in,
                                                                                                   e))
                update(construct_increment_bericht_attempts_query(graph, bericht['uri']))
                continue
            if post_result:
                ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat() # We consider the moment when the api-call succeeded the 'ontvangen'-time
                q_sent = construct_bericht_sent_query(graph, bericht['uri'], ontvangen)
                update(q_sent)
                log("successfully sent bericht {} with {} bijlagen to Kalliope".format(bericht['uri'],
                                                                                       len(bijlagen)))
    pass
