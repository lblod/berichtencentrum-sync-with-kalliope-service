import os
from datetime import datetime, timedelta
from pytz import timezone
import helpers
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import parse_kalliope_poststuk_uit
from .kalliope_adapter import  construct_kalliope_poststuk_in
from .kalliope_adapter import  open_kalliope_api_session
from .kalliope_adapter import  get_kalliope_poststukken_uit
from .kalliope_adapter import  post_kalliope_poststuk_in
from .queries import construct_bericht_exists_query
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_bijlage_query
from .queries import construct_insert_conversatie_query
from .queries import construct_insert_bericht_query
from .queries import construct_unsent_berichten_query
from .queries import construct_select_bijlagen_query
from .queries import construct_bericht_sent_query
from .queries import construct_update_last_bericht_query_part1
from .queries import construct_update_last_bericht_query_part2
from .queries import construct_select_original_bericht_query
from .kalliope_adapter import BIJLAGEN_FOLDER_PATH

TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
PS_UIT_PATH = os.environ.get('KALLIOPE_PS_UIT_ENDPOINT')
PS_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_MESSAGE_AGE = int(os.environ.get('MAX_MESSAGE_AGE')) #in days

def process_berichten_in():
    """
    Fetch Berichten from the Kalliope-api, parse them, and if needed, import them into the triple store.

    :returns:
    """
    vanaf = datetime.now(tz=TIMEZONE) - timedelta(days=MAX_MESSAGE_AGE)
    tot = datetime.now(tz=TIMEZONE)
    log("Pulling poststukken from kalliope API for period {} - {}".format(vanaf.isoformat(), tot.isoformat()))
    api_query_params = {
        'vanaf': vanaf.isoformat(),
        'tot': tot.isoformat(),
        # 'dossierTypes': "https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/klacht",
        'aantal': str(1000)
    }
    with open_kalliope_api_session(verify=False) as session: # WARNING: Certificate validity isn't verified atm
        try:
            poststukken = get_kalliope_poststukken_uit(PS_UIT_PATH, session, api_query_params)
            log('Retrieved {} poststukken uit from Kalliope'.format(len(poststukken)))
        except Exception as e:
            log("Something went wrong while accessing the Kalliope API. Aborting: {}".format(e))
            return
        
        for poststuk in poststukken:
            try:
                (conversatie, bericht) = parse_kalliope_poststuk_uit(poststuk, session)
            except Exception as e:
                log("Something went wrong parsing following poststuk uit, skipping: {}\n{}".format(poststuk, e))
                continue
            bestuurseenheid_uuid = bericht['naar'].split('/')[-1]
            graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
            q = construct_bericht_exists_query(graph, bericht['uri'])
            query_result = query(q)['results']['bindings']
            if not query_result: #Bericht is not in our DB yet. We should insert it.
                log("Bericht '{}' - {} is not in DB yet.".format(conversatie['betreft'], bericht['verzonden']))
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
                        q_bijlage = construct_insert_bijlage_query(graph, PUBLIC_GRAPH, bericht['uri'], bijlage, file) # TEMP: bijlage in public graph
                        result = update(q_bijlage)
                if conversatie['dossiernummer']:
                    q2 = construct_conversatie_exists_query(graph, conversatie['dossiernummer'])
                    query_result2 = query(q2)['results']['bindings']
                    if query_result2: #conversatie to which the bericht is linked, exists.
                        conversatie_uri = query_result2[0]['conversatie']['value']
                        log("Existing conversation '{}' inserting new message sent @ {}".format(conversatie['betreft'], bericht['verzonden']))
                        q_bericht = construct_insert_bericht_query(graph, bericht, conversatie_uri)
                        result = update(q_bericht)
                        save_bijlagen(bericht['bijlagen'])
                    else: #conversatie to which the bericht is linked does not exist yet.
                        log("Non-existing conversation '{}' inserting new conversation + message sent @ {}".format(conversatie['betreft'], bericht['verzonden']))
                        conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
                        q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht)
                        result = update(q_conversatie)
                        save_bijlagen(bericht['bijlagen'])
                else: # TEMP: Message without linked dossier
                    log("Non-existing conversation '{}' inserting new conversation + message sent @ {} (No dossier linked to message!)".format(conversatie['betreft'], bericht['verzonden']))
                    conversatie['uri'] = "http://data.lblod.info/id/conversaties/{}".format(conversatie['uuid'])
                    q_conversatie = construct_insert_conversatie_query(graph, conversatie, bericht)
                    result = update(q_conversatie)
                    save_bijlagen(bericht['bijlagen'])
                # Updating ext:lastMessage link for each conversation (in 2 parts because Virtuoso)
                update(construct_update_last_bericht_query_part1())
                update(construct_update_last_bericht_query_part2())
            else: #bericht already exists in our DB
                log("Bericht '{}' - {} already exists in our DB, skipping ...".format(conversatie['betreft'], bericht['verzonden']))
                pass
    
def process_berichten_out():
    """
    Fetch Berichten that have to be sent from the triple store, convert them to the correct format for the Kalliope API, post them and finally mark them as sent.

    :param ?:
    :returns:
    """
    q = construct_unsent_berichten_query(ABB_URI)
    berichten = query(q)['results']['bindings']
    log("Found {} berichten that need to be sent to the Kalliope API".format(len(berichten)))
    if len(berichten) == 0:
        return
    with open_kalliope_api_session(verify=False) as session: # WARNING: Certificate validity isn't verified atm
        for bericht_res in berichten:
            bericht = {
                'uri': bericht_res['bericht']['value'],
                'van': bericht_res['van']['value'],
                'verzonden': bericht_res['verzonden']['value'],
                'inhoud': bericht_res['inhoud']['value'],
            }
            q_origineel = construct_select_original_bericht_query(bericht['uri'])
            origineel_bericht_uri = query(q_origineel)['results']['bindings'][0]['origineelbericht']['value']
            conversatie = {
                'betreft': bericht_res['betreft']['value'],
                'origineelBerichtUri': origineel_bericht_uri
            }
            if 'dossiernummer' in bericht_res:
                conversatie['dossiernummer'] = bericht_res['dossiernummer']['value']
            if 'dossieruri' in bericht_res:
                conversatie['dossierUri'] = bericht_res['dossieruri']['value'] # TEMP: As kalliope identifier for Dossier while dossiernummer doesn't exist
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
            log("Posting bericht <{}>. Payload: {}".format(bericht['uri'], poststuk_in))
            post_result = post_kalliope_poststuk_in(PS_IN_PATH, session, poststuk_in)
            if post_result:
                ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat() # We consider the moment when the api-call succeeded the 'ontvangen'-time
                bestuurseenheid_uuid = bericht['van'].split('/')[-1] # NOTE: Add graph as argument to query because Virtuoso
                graph = "http://mu.semte.ch/graphs/organizations/{}/LoketLB-berichtenGebruiker".format(bestuurseenheid_uuid)
                q_sent = construct_bericht_sent_query(graph, bericht['uri'], ontvangen)
                update(q_sent)
                log("successfully sent bericht {} with {} bijlagen to Kalliope".format(bericht['uri'], len(bijlagen)))
    pass
