import os
from pytz import timezone
from datetime import datetime
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import post_kalliope_inzending_in
from .kalliope_adapter import open_kalliope_api_session
from .queries import construct_unsent_inzendingen_query
from .queries import construct_increment_inzending_attempts_query
from .queries import construct_inzending_sent_query
from .queries import construct_create_kalliope_sync_error_query
from .queries import verify_eb_has_cb_exclusion_rule
from .queries import verify_eb_exclusion_rule
from .queries import verify_cb_exclusion_rule
from .queries import verify_ro_exclusion_rule
from .queries import verify_go_exclusion_rule
from .queries import verify_po_exclusion_rule
from .update_with_supressed_fail import update_with_suppressed_fail
from dateutil import parser



TIMEZONE = timezone('Europe/Brussels')
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))
INZENDING_BASE_URL = os.environ.get('INZENDING_BASE_URL')
EREDIENSTEN_BASE_URL = os.environ.get('EREDIENSTEN_BASE_URL')


def process_inzendingen():
    """
    Fetch submissions that have to be sent to Kalliope from the triple store,
        convert them to the correct format for the Kalliope API, post them and finally mark them as sent.
    :returns: None
    """
    q = construct_unsent_inzendingen_query(MAX_SENDING_ATTEMPTS)
    inzendingen = query(q)['results']['bindings']

    # Here we remove inzendingen that matches exclusion criteria from business rules 
    filtered_inzendingen = exclude_inzendingen_from_rules(inzendingen)

    inzendingen = [parse_inzending_sparql_response(inzending_res) for inzending_res in filtered_inzendingen]
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
    session_date = inzending_res.get('sessionDate', {}).get('value', '')
    if session_date:
        session_date = parser.isoparse(session_date)
        session_date = session_date.astimezone(TIMEZONE)
        session_date = session_date.strftime('%Y-%m-%d')

    erediensten_databank_flow_only = inzending_res['decisionType']['value'] in ['https://data.vlaanderen.be/id/concept/BesluitDocumentType/14793940-5b9c-4172-b108-c73665ad9d6a', 'https://data.vlaanderen.be/id/concept/BesluitDocumentType/651525f8-8650-4ce8-8eea-f19b94d50b73']

    inzending = {
        'uri': inzending_res['inzending']['value'],
        'afzenderUri': inzending_res['bestuurseenheid']['value'],
        'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' +
          session_date,
        'urlToezicht': EREDIENSTEN_BASE_URL + '/' + inzending_res['inzendingUuid']['value'] if erediensten_databank_flow_only else INZENDING_BASE_URL + '/' + inzending_res['inzendingUuid']['value'],
        'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
        'typeMelding': inzending_res['decisionType']['value'],
        'datumVanVerzenden': inzending_res['datumVanVerzenden']['value']
    }

    #  NOTE: Kalliope expects "boekjaar" to be an int.
    #        At this stage we can not guarantee this to be true.
    try:
        value = inzending_res.get('boekjaar', {}).get('value', '')
        if value != "":
            inzending['boekjaar'] = int(value)
    except ValueError:
        log("Invalid value \"{}\" for boekjaar will be ignored, expected an int.".format(value))

    return inzending

def exclude_inzendingen_from_rules(inzendingen):
    """
    This takes an individual submission to run ASK queries to check if it matches the pattern from business rules (a submission's formData who has a specific decisionType and sender needs to be excluded when they match a certain criteria in the list); 
    It will then sort them out from the inzendingen.
    see: Leesrechtenlogica Databank Erediensten
    """
    filtered_inzendingen = []

    for inzending in inzendingen:

        submission = inzending['inzending']['value']

        eb_has_cb = query(verify_eb_has_cb_exclusion_rule(submission))['boolean']
        eb = query(verify_eb_exclusion_rule(submission))['boolean']
        cb = query(verify_cb_exclusion_rule(submission))['boolean']
        ro = query(verify_ro_exclusion_rule(submission))['boolean']
        go = query(verify_go_exclusion_rule(submission))['boolean']
        po = query(verify_po_exclusion_rule(submission))['boolean']


        if not (eb_has_cb or eb or cb or ro or go or po):
            filtered_inzendingen.append(inzending)

    return filtered_inzendingen