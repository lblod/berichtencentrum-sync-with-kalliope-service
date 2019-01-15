#!/usr/bin/python3
from datetime import datetime
import re
import os
import requests
import magic
import helpers, escape_helpers

ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"

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

def get_kalliope_bijlage(path, auth):
    """
    Perform the API-call to get a poststuk-uit bijlage.

    :param path: url of the api endpoint
    :param auth: tuple of the form ('user', 'pass')
    :param url_params: dict of url parameters for the api call
    :returns: buffer with bijlage
    """
    r = requests.get(path, auth=auth, verify=False) # WARNING: Certificate validity isn't verified atm
    r.connection.close()
    if r.status_code == requests.codes.ok:
        return r.content
    else:
        raise requests.exceptions.HTTPError('Failed to get Kalliope poststuk bijlage (statuscode {})'.format(r.status_code))
    
def parse_kalliope_bijlage(ps_bijlage):
    """
    Parse the mijlage response from the Kalliope API into our bijlage format

    :param bijlage: The bijlage deserialized JSON
    :returns: a dict of bijlage properties including the binary buffer
    """
    api_auth = (os.environ.get('KALLIOPE_API_USERNAME'), os.environ.get('KALLIOPE_API_PASSWORD'))
    buffer = get_kalliope_bijlage(ps_bijlage['url'], api_auth)
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
        'created': datetime.utcnow().isoformat()+'Z',
    }
    return bijlage

def get_kalliope_poststukken_uit(path, auth, url_params):
    """
    Perform the API-call to get all poststukken-uit that are ready to be processed.

    :param path: url of the api endpoint
    :param auth: tuple of the form ('user', 'pass')
    :param url_params: dict of url parameters for the api call
    :returns: tuple of poststukken
    """
    r = requests.get(path, auth=auth, params=url_params, verify=False) # WARNING: Certificate validity isn't verified atm
    r.connection.close()
    if r.status_code == requests.codes.ok:
        poststukken = r.json()['poststukken']
        # TODO: paged response 
        return tuple(poststukken)
    else:
        raise requests.exceptions.HTTPError('Failed to get Kalliope poststuk uit (statuscode {})'.format(r.status_code))
    
def parse_kalliope_poststuk_uit(ps_uit):
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
    def isotz_repl(matchobj): # HACK: making ISO 8601 timezone offset xsd-compliant (including colon)
        hh = matchobj.group(1)[0:2]
        mm = matchobj.group(1)[2:4]
        return "+{}:{}".format(hh, mm)
    verzonden = re.sub(r'\+(\d{4})', isotz_repl, ps_uit['creatieDatum']) 
    ontvangen = datetime.utcnow().isoformat()+'Z'
    inhoud = ps_uit['inhoud']
    dossiernummer = ps_uit['dossier']['naam'] # NOTE: Will become "dossierNummer" in future API versiom 
    betreft = ps_uit['betreft']
    type_communicatie = ps_uit['typeCommunicatie']
    reactietermijn = "P30D"

    bericht = new_bericht(verzonden, ontvangen, van, naar, inhoud)
    bericht['uri'] = ps_uit['uri']
    conversatie = new_conversatie(dossiernummer,
                        betreft,
                        type_communicatie,
                        reactietermijn)

    bericht['bijlagen'] = []
    for ps_bijlage in ps_uit['bijlages']:
        try:
            bijlage = parse_kalliope_bijlage(ps_bijlage)
            bericht['bijlagen'].append(bijlage)
        except Exception as e:
            helpers.log("Something went wrong while parsing a bijlage for bericht {} sent @ {}".format(conversatie['betreft'], bericht['verzonden'])) 

    return (conversatie, bericht)

def construct_kalliope_poststuk_in(arg):
    """
    Prepare the payload for sending messages to the Kalliope API.

    :param ?:
    :returns:
    """
    pass

def post_kalliope_poststuk_in(arg):
    """
    Perform the API-call to a new poststuk to Kalliope.

    :param ?:
    :returns:
    """
    pass
