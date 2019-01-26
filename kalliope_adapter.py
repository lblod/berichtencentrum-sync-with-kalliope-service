#!/usr/bin/python3
from datetime import datetime
from pytz import timezone
import re
import os
import requests
import magic
import helpers, escape_helpers

TIMEZONE = timezone('Europe/Brussels')
ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
BIJLAGEN_FOLDER_PATH = "/data/files"

def new_conversatie(dossiernummer,
                    betreft,
                    type_communicatie,
                    reactietermijn,
                    berichten=[]):
    conversatie = {}
    conversatie['uuid'] = helpers.generate_uuid()
    conversatie['dossiernummer'] = dossiernummer
    conversatie['betreft'] = betreft
    conversatie['type_communicatie'] = type_communicatie
    conversatie['reactietermijn'] = reactietermijn
    conversatie['berichten'] = list(berichten)
    return conversatie

def new_bericht(verzonden,
                ontvangen,
                van,
                naar,
                inhoud):
    bericht = {}
    bericht['uuid'] = helpers.generate_uuid()
    bericht['verzonden'] = verzonden
    bericht['ontvangen'] = ontvangen
    bericht['van'] = van
    bericht['naar'] = naar
    bericht['inhoud'] = inhoud
    return bericht

def open_kalliope_api_session(verify=False): # WARNING: Certificate validity isn't verified atm
    s = requests.Session()
    s.auth = (os.environ.get('KALLIOPE_API_USERNAME'), os.environ.get('KALLIOPE_API_PASSWORD'))
    s.verify = verify
    return s

def get_kalliope_bijlage(path, session):
    """
    Perform the API-call to get a poststuk-uit bijlage.

    :param path: url of the api endpoint
    :param auth: tuple of the form ('user', 'pass')
    :param url_params: dict of url parameters for the api call
    :returns: buffer with bijlage
    """
    r = session.get(path)
    if r.status_code == requests.codes.ok:
        return r.content
    else:
        raise requests.exceptions.HTTPError('Failed to get Kalliope poststuk bijlage (statuscode {})'.format(r.status_code))
    
def parse_kalliope_bijlage(ps_bijlage, session):
    """
    Parse the mijlage response from the Kalliope API into our bijlage format

    :param bijlage: The bijlage deserialized JSON
    :returns: a dict of bijlage properties including the binary buffer
    """
    buffer = get_kalliope_bijlage(ps_bijlage['url'], session)
    m_type = magic.Magic(mime=True)
    m_encoding = magic.Magic(mime_encoding=True) #mimeype and encoding seperate because of library quirk
    filesize = len(buffer)
    bijlage = {
        'uuid': helpers.generate_uuid(),
        'buffer': buffer,
        'url': ps_bijlage['url'],
        'id': ps_bijlage['url'].split('/')[-1],
        'name': ps_bijlage['naam'],
        'extension': os.path.splitext(ps_bijlage['naam'])[1].strip("."),
        'mimetype': m_type.from_buffer(buffer) + "; charset=" + m_encoding.from_buffer(buffer),
        'size': filesize,
        'created': datetime.now().isoformat(),
    }
    return bijlage

def get_kalliope_poststukken_uit(path, session, url_params):
    """
    Perform the API-call to get all poststukken-uit that are ready to be processed.

    :param path: url of the api endpoint
    :param auth: tuple of the form ('user', 'pass')
    :param url_params: dict of url parameters for the api call
    :returns: tuple of poststukken
    """
    r = session.get(path, params=url_params)
    if r.status_code == requests.codes.ok:
        poststukken = r.json()['poststukken']
        # TODO: paged response 
        return tuple(poststukken)
    else:
        raise requests.exceptions.HTTPError('Failed to get Kalliope poststuk uit (statuscode {})'.format(r.status_code))
    
def parse_kalliope_poststuk_uit(ps_uit, session):
    """
    Parse the response from the Kalliope API into our bericht format

    :param ps_uit: The poststuk uit deserialized JSON
    :returns: a tuple of the form (conversatie, bericht)
    """
    van = ABB_URI
    if ps_uit['bestemmeling']['uri']:
        naar = ps_uit['bestemmeling']['uri']
    else:
        raise ValueError("The bestemmeling has no URI. Probably this message isn't intended for Loket")
    def pythonize_iso_timestamp(timestamp):
        """ Convert ISO 8601 timestamp to python .fromisoformat()-compliant format """
        def repl(matchobj):
            hh, mm = matchobj.group(1)[0:2], matchobj.group(1)[2:4]
            return "+{}:{}".format(hh, mm)
        return re.sub(r'\+(\d{4})', repl, timestamp)
    verzonden =  datetime.fromisoformat(pythonize_iso_timestamp(ps_uit['creatieDatum'])).astimezone(TIMEZONE).isoformat()
    ontvangen = datetime.now(tz=TIMEZONE).isoformat()
    inhoud = ps_uit['inhoud'] if ps_uit['inhoud'] else ""
    dossiernummer = ps_uit['dossier']['naam'] # NOTE: Will become ps_uit['dossierNummer'] in future API version
    betreft = ps_uit['betreft']
    type_communicatie = ps_uit['typeCommunicatie']
    reactietermijn = "P30D"

    bericht = new_bericht(verzonden, ontvangen, van, naar, inhoud)
    bericht['uri'] = ps_uit['uri']
    conversatie = new_conversatie(dossiernummer,
                        betreft,
                        type_communicatie,
                        reactietermijn)
    conversatie['dossierUri'] =  ps_uit['dossier']['uri'] # TEMP: As kalliope identifier for Dossier while dossiernummer doesn't exist

    bericht['bijlagen'] = []
    for ps_bijlage in ps_uit['bijlages']:
        try:
            bijlage = parse_kalliope_bijlage(ps_bijlage, session)
            bericht['bijlagen'].append(bijlage)
        except Exception as e:
            helpers.log("Something went wrong while parsing a bijlage for bericht {} sent @ {}".format(conversatie['betreft'], bericht['verzonden'])) 

    return (conversatie, bericht)

def construct_kalliope_poststuk_in(conversatie, bericht):
    """
    Prepare the payload for sending messages to the Kalliope API.

    :param ?:
    :returns:
    """
    files = []
    for bijlage in bericht['bijlagen']:
        filepath = os.path.join(BIJLAGEN_FOLDER_PATH, bijlage['filepath'])
        buffer = open(filepath, 'rb')
        files.append(('files', (bijlage['name'], buffer, bijlage['type']))) # http://docs.python-requests.org/en/master/user/advanced/#post-multiple-multipart-encoded-files

    # NOTE: All parameters are sent as file-like objects because the API expects a 'Content-Type'-header for each parameter
    poststuk_in = [
        ('uri', (None, bericht['uri'], 'text/plain')),
        ('afzender_uri', (None, bericht['van'], 'text/plain')),
        ('dossier_uri', (None, conversatie['dossierUri'], 'text/plain')), # NOTE: optional # TEMP: As kalliope identifier for Dossier while dossiernummer doesn't exist
        ('origineel_bericht_uri', (None, conversatie['origineelBerichtUri'], 'text/plain')), # NOTE: optional # TEMP: As kalliope identifier for Dossier while dossiernummer doesn't exist
        ('betreft', (None, conversatie['betreft'], 'text/plain')), # NOTE: Is always the same across the whole conversation for what we are concerned 
        # 'origineelBerichtUri': conversatie['berichten'][0]['uri'], # NOTE: optional
        ('inhoud', (None, bericht['inhoud'], 'text/plain')), # NOTE: optional
    ]
    poststuk_in.extend(files)
    return poststuk_in

def post_kalliope_poststuk_in(path, session, params):
    """
    Perform the API-call to a new poststuk to Kalliope.

    :param ?:
    :returns:
    """
    r = session.post(path, files=params)
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        raise requests.exceptions.HTTPError('Failed to post Kalliope poststuk-in (statuscode {}): {}'.format(r.status_code, r.json()))
