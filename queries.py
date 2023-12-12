#!/usr/bin/python3
import copy
import escape_helpers
import helpers
from helpers import log
from datetime import datetime
from pytz import timezone
import re
import json

CONFIG_FILE_PATH = '/config/config.json'
TIMEZONE = timezone('Europe/Brussels')
STATUS_DELIVERED_UNCONFIRMED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/unconfirmed"
STATUS_DELIVERED_CONFIRMED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/confirmed"
STATUS_DELIVERED_CONFIRMATION_FAILED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/failedConfirmation"

# Inzendingen business rules :  sender classification with decisionType

DECISION_TYPES_EB_HAS_CB = [
    "<https://data.vlaanderen.be/id/concept/BesluitType/e44c535d-4339-4d15-bdbf-d4be6046de2c>", # Jaarrekening
    "<https://data.vlaanderen.be/id/concept/BesluitDocumentType/2c9ada23-1229-4c7e-a53e-acddc9014e4e>" # Gecoordineerde inzending meerjarenplannen
]

DECISION_TYPES_EB = [
    "<https://data.vlaanderen.be/id/concept/BesluitType/f56c645d-b8e1-4066-813d-e213f5bc529f>" # Meerjarenplan(aanpassing)
]

DECISION_TYPES_CB = [
    "<https://data.vlaanderen.be/id/concept/BesluitDocumentType/18833df2-8c9e-4edd-87fd-b5c252337349>", # Budgetten(wijzigingen) - Indiening bij representatief orgaan
    "<https://data.vlaanderen.be/id/concept/BesluitDocumentType/2c9ada23-1229-4c7e-a53e-acddc9014e4e>" # Gecoordineerde inzending meerjarenplannen
]

DECISION_TYPES_RO = [
    "<https://data.vlaanderen.be/id/concept/BesluitType/2b12630f-8c4e-40a4-8a61-a0c45621a1e6>", # Advies Budget(wijziging)
    "<https://data.vlaanderen.be/id/concept/BesluitType/0fc2c27d-a03c-4e3f-9db1-f10f026f76f8>" # Advies Meerjarenplan
]

DECISION_TYPES_GO = [
    "<https://data.vlaanderen.be/id/concept/BesluitType/df261490-cc74-4f80-b783-41c35e720b46>", # Besluit over budget(wijziging) eredienstbestuur
    "<https://data.vlaanderen.be/id/concept/BesluitType/3fcf7dba-2e5b-4955-a489-6dd8285c013b>" # Besluit over meerjarenplan(aanpassing) eredienstbestuur
]

DECISION_TYPES_PO = [
    "<https://data.vlaanderen.be/id/concept/BesluitType/df261490-cc74-4f80-b783-41c35e720b46>", # Besluit over budget(wijziging) eredienstbestuur
    "<https://data.vlaanderen.be/id/concept/BesluitType/3fcf7dba-2e5b-4955-a489-6dd8285c013b>" # Besluit over meerjarenplan(aanpassing) eredienstbestuur
]


def sparql_escape_string(obj):
    log("""Warning: using a monkey patched
         sparql_escape_string.
         TODO: move this to template""")

    obj = str(obj)

    def replacer(a):
        return "\\"+a.group(0)

    return '"""' + re.sub(r'[\\\"]', replacer, obj) + '"""'


# monkey patch escape_helpers. TODO: mov
escape_helpers.sparql_escape_string = sparql_escape_string


def construct_conversatie_exists_query(graph_uri, referentieABB):
    """
    Construct a query for selecting a conversatie based on referentieABB
    (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param referentieABB: string
    :returns: string containing SPARQL query
    """
    referentieABB = escape_helpers.sparql_escape_string(referentieABB)
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie
        WHERE {{
            GRAPH <{}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier {}.
            }}
        }}
        """.format(graph_uri, referentieABB)
    return q


def construct_bestuurseenheid_exists_query(bestuurseeheid_uri):
    """
    Construct a query for asking if a bestuurseenehid exists in our database.

    :param bestuurseeheid_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

        ASK {{
            <{0}> a besluit:Bestuurseenheid .
        }}
        """.format(bestuurseeheid_uri)
    return q


def construct_bericht_exists_query(graph_uri, bericht_uri):
    """
    Construct a query for selecting a bericht based on its URI, retrieving the conversatie & referentieABB at the same time.

    :param graph_uri: string
    :param bericht_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie ?referentieABB
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                ?conversatie a schema:Conversation;
                    schema:hasPart <{1}>;
                    schema:identifier ?referentieABB.
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q


def construct_insert_conversatie_query(graph_uri, conversatie, bericht, delivery_timestamp):
    """
    Construct a SPARQL query for inserting a new conversatie with a first bericht attached.

    :param graph_uri: string
    :param conversatie: dict containing escaped properties for conversatie
    :param bericht: dict containing escaped properties for bericht
    :returns: string containing SPARQL query
    """
    conversatie = copy.deepcopy(conversatie)  # For not modifying the pass-by-name original
    conversatie['referentieABB'] = escape_helpers.sparql_escape_string(conversatie['referentieABB'])
    conversatie['betreft'] = escape_helpers.sparql_escape_string(conversatie['betreft'])
    conversatie['current_type_communicatie'] =\
        escape_helpers.sparql_escape_string(conversatie['current_type_communicatie'])
    bericht = copy.deepcopy(bericht)  # For not modifying the pass-by-name original
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier {1[referentieABB]};
     """
    if conversatie["dossierUri"]:
        q += """
                        ext:dossierUri "{1[dossierUri]}";
             """
    q += """
                    schema:about {1[betreft]};
                    <http://mu.semte.ch/vocabularies/ext/currentType> {1[current_type_communicatie]};
                    schema:processingTime "{1[reactietermijn]}";
                    schema:hasPart <{2[uri]}>.

                <{2[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    schema:dateSent "{2[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{2[ontvangen]}"^^xsd:dateTime;
                    schema:text "Origineel bericht in bijlage";
                    <http://purl.org/dc/terms/type> "{2[type_communicatie]}";
                    schema:sender <{2[van]}>;
                    schema:recipient <{2[naar]}>;
                    adms:status <{3}>;
                    ext:deliveredAt "{4}"^^xsd:dateTime.
            }}
        }}
    """
    q = q.format(graph_uri, conversatie, bericht, STATUS_DELIVERED_UNCONFIRMED, delivery_timestamp)
    return q


def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri, delivery_timestamp):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :param delivery_timestamp: string
    :returns: string containing SPARQL query
    """
    bericht = copy.deepcopy(bericht)  # For not modifying the pass-by-name original
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{2}> a schema:Conversation;
                    schema:hasPart <{1[uri]}>.
                <{1[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:dateSent "{1[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{1[ontvangen]}"^^xsd:dateTime;
                    schema:text "Origineel bericht in bijlage";
                    schema:sender <{1[van]}>;
                    schema:recipient <{1[naar]}>;
                    adms:status <{3}>;
                    ext:deliveredAt "{4}"^^xsd:dateTime;
                    <http://purl.org/dc/terms/type> "{1[type_communicatie]}".
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri,
                   STATUS_DELIVERED_UNCONFIRMED, delivery_timestamp)
    return q


def construct_update_conversatie_type_query(graph_uri, conversatie_uri, type_communicatie):
    """
    Construct a SPARQL query for updating the type-communicatie of a conversatie.

    :param graph_uri: string
    :param conversatie_uri: string containing the uri of the conversatie we want to update
    :param type_communicatie: string containing the type-communicatie

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> ?type.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> "{2}".
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> ?type.
            }}
        }}
        """.format(graph_uri, conversatie_uri, type_communicatie)
    return q


def construct_insert_bijlage_query(bericht_graph_uri, bericht_uri, bijlage, file):
    """
    Construct a SPARQL query for inserting a bijlage and attaching it to an existing bericht.

    :param bericht_graph_uri: string
    :param bericht_uri: string
    :param bijlage: dict containing escaped properties for bijlage
    :param file: dict containing escaped properties for file (similar to bijlage, see mu-file-service)
    :returns: string containing SPARQL query
    """
    bijlage = copy.deepcopy(bijlage) # For not modifying the pass-by-name original
    bijlage['name'] = escape_helpers.sparql_escape_string(bijlage['name'])
    bijlage['mimetype'] = escape_helpers.sparql_escape_string(bijlage['mimetype'])
    file = copy.deepcopy(file) # For not modifying the pass-by-name original
    file['name'] = escape_helpers.sparql_escape_string(file['name'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX dbpedia: <http://dbpedia.org/ontology/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1}> nie:hasPart <{2[uri]}>.
                <{2[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    nfo:fileName {2[name]};
                    dct:format {2[mimetype]};
                    dct:created "{2[created]}"^^xsd:dateTime;
                    nfo:fileSize "{2[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{2[extension]}".
                <{3[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{3[uuid]}";
                    nfo:fileName {3[name]};
                    dct:format {2[mimetype]};
                    dct:created "{2[created]}"^^xsd:dateTime;
                    nfo:fileSize "{2[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{2[extension]}";
                    nie:dataSource <{2[uri]}>.
            }}
        }}
        """.format(bericht_graph_uri, bericht_uri, bijlage, file)
    return q


def construct_update_last_bericht_query(conversatie_uri):
    """
    Construct a SPARQL query for keeping the last message of a conversation up to date.

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {{
            GRAPH ?g {{
                ?conversation ext:lastMessage ?message.
            }}
        }}
        INSERT {{
            GRAPH ?g {{
                ?conversation ext:lastMessage ?newMessage.
            }}
        }}
        WHERE {{
            BIND(<{0}> as ?conversation)
            GRAPH ?g {{
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage.
                OPTIONAL {{  ?conversation ext:lastMessage ?message. }}
            }}
            {{
              SELECT DISTINCT (?message AS ?newMessage) ?dateSent WHERE {{
                ?conversation a schema:Conversation;
                  schema:hasPart ?message.

                ?message schema:dateSent ?dateSent.
              }}
              ORDER BY DESC(?dateSent)
              LIMIT 1
            }}
        }}
        """.format(conversatie_uri)
    return q


def construct_unsent_berichten_query(naar_uri, max_sending_attempts):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param naar_uri: URI of the recipient for which we want to retrieve messages that have yet to be sent.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        SELECT DISTINCT ?referentieABB ?dossieruri ?bericht ?betreft ?uuid ?van ?verzonden ?inhoud
        WHERE {{
                ?conversatie a schema:Conversation;
                    schema:identifier ?referentieABB;
                    schema:about ?betreft;
                    schema:hasPart ?bericht.
                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?uuid;
                    schema:dateSent ?verzonden;
                    schema:text ?inhoud;
                    schema:sender ?van;
                    schema:recipient <{0}>.
                FILTER NOT EXISTS {{ ?bericht schema:dateReceived ?ontvangen. }}
                OPTIONAL {{
                    ?conversatie ext:dossierUri ?dossieruri.
                }}
                BIND(0 AS ?default_attempts)
                OPTIONAL {{ ?bericht ext:failedSendingAttempts ?attempts. }}
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                FILTER(?result_attempts < {1})
        }}
        """.format(naar_uri, max_sending_attempts)
    return q


def construct_select_bijlagen_query(bericht_uri):
    """
    Construct a SPARQL query for retrieving all bijlages for a given bericht.

    :param bericht_uri: URI of the bericht for which we want to retrieve bijlagen.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?bijlagenaam ?file ?type WHERE {{
            <{0}> a schema:Message;
                nie:hasPart ?bijlage.

            ?bijlage a nfo:FileDataObject;
                nfo:fileName ?bijlagenaam;
                dct:format ?type.
            ?file nie:dataSource ?bijlage.
        }}
        """.format(bericht_uri)
    return q


def construct_increment_bericht_attempts_query(graph_uri, bericht_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a certain message without succes.

    :param graph_uri: string
    :param bericht_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                OPTIONAL {{ <{1}> ext:failedSendingAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q


def construct_bericht_sent_query(graph_uri, bericht_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party (and thus 'sent' by us)

    :param graph_uri: string
    :param bericht_uri: URI of the bericht we would like to mark as sent.
    :param verzonden: ISO-string representation of the datetetime when the message was sent
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT {{
            GRAPH <{0}> {{
                <{1}> schema:dateReceived "{2}"^^xsd:dateTime.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
            }}
        }}
        """.format(graph_uri, bericht_uri, verzonden)
    return q


def construct_select_original_bericht_query(bericht_uri):
    """
    Construct a SPARQL query for selecting the first message in a conversation

    :param bericht_uri: URI of a bericht in a conversation
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        SELECT DISTINCT ?origineelbericht ?dateSent WHERE {{
          ?conversation a schema:Conversation;
            schema:hasPart ?origineelbericht;
            schema:hasPart <{0}>.

            ?origineelbericht schema:dateSent ?dateSent.
         }}
         ORDER BY ASC(?dateSent)
         LIMIT 1
       """.format(bericht_uri)

    return q

def verify_eb_has_cb_exclusion_rule(submission):

    ask_query_eb_has_cb = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX org:         <http://www.w3.org/ns/org#>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{

        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}

        ?bestuurseenheid a ere:BestuurVanDeEredienst.

        ?centraalBestuur a ere:CentraalBestuurVanDeEredienst ;
                         org:hasSubOrganization ?bestuurseenheid .
    }}
    """.format(submission, " ".join(DECISION_TYPES_EB_HAS_CB))

    return ask_query_eb_has_cb

def verify_eb_exclusion_rule(submission):

    ask_query_eb = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{

        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}

        ?bestuurseenheid a ere:BestuurVanDeEredienst.

    }}
    """.format(submission, " ".join(DECISION_TYPES_EB))

    return ask_query_eb

def verify_cb_exclusion_rule(submission):

    ask_query_cb = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX org:         <http://www.w3.org/ns/org#>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{
        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}

        ?bestuurseenheid a ere:CentraalBestuurVanDeEredienst .
    }}
    """.format(submission, " ".join(DECISION_TYPES_CB))

    return ask_query_cb

def verify_ro_exclusion_rule(submission):

    ask_query_ro = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX org:         <http://www.w3.org/ns/org#>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{
        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}

        ?bestuurseenheid a ere:RepresentatiefOrgaan .
    }}
    """.format(submission, " ".join(DECISION_TYPES_RO))

    return ask_query_ro

def verify_go_exclusion_rule(submission):

    ask_query_go = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX org:         <http://www.w3.org/ns/org#>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX besluit:     <http://data.vlaanderen.be/ns/besluit#>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{
        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}

        ?bestuurseenheid besluit:classificatie <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000001> .
    }}
    """.format(submission, " ".join(DECISION_TYPES_GO))

    return ask_query_go

def verify_po_exclusion_rule(submission):

    ask_query_po = """
    PREFIX ere:         <http://data.lblod.info/vocabularies/erediensten/>
    PREFIX org:         <http://www.w3.org/ns/org#>
    PREFIX pav:         <http://purl.org/pav/>
    PREFIX meb:         <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX dct:         <http://purl.org/dc/terms/>
    PREFIX besluit:     <http://data.vlaanderen.be/ns/besluit#>
    PREFIX prov:        <http://www.w3.org/ns/prov#>
    PREFIX adms:        <http://www.w3.org/ns/adms#>
    
    ASK {{
        BIND(<{0}> AS ?submission)
        ?submission a meb:Submission ;
                   adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                   prov:generated ?formData ;
                   pav:createdBy ?bestuurseenheid .
        ?formData dct:type ?decisionType .
        VALUES ?decisionType {{ {1} }}
        
        ?bestuurseenheid besluit:classificatie <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000000> .
    }}
    """.format(submission, " ".join(DECISION_TYPES_PO))

    return ask_query_po


def construct_unsent_inzendingen_query(max_sending_attempts):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param max_sending_attempts: the maximum number of delivery attempts that have to be done
    :returns: string containing SPARQL query
    """
    with open(CONFIG_FILE_PATH) as config_file:
        allowedDecisionTypesList = json.load(config_file)['allowedDecisionTypes'];

    separator = ' '

    q = """
        PREFIX mu:      <http://mu.semte.ch/vocabularies/core/>
        PREFIX meb:     <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX dct:     <http://purl.org/dc/terms/>
        PREFIX nmo:     <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX ext:     <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms:    <http://www.w3.org/ns/adms#>
        PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
        PREFIX prov:    <http://www.w3.org/ns/prov#>
        PREFIX eli:     <http://data.europa.eu/eli/ontology#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
        PREFIX ere:     <http://data.lblod.info/vocabularies/erediensten/>
        PREFIX org:     <http://www.w3.org/ns/org#>
        PREFIX pav:     <http://purl.org/pav/>
        
        SELECT DISTINCT ?inzending ?inzendingUuid ?bestuurseenheid ?decisionType ?sessionDate
                        ?decisionTypeLabel ?datumVanVerzenden ?boekjaar
        WHERE {{
            GRAPH ?g {{
                ?inzending a meb:Submission ;
                    adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                    mu:uuid ?inzendingUuid ;
                    pav:createdBy ?bestuurseenheid;
                    nmo:sentDate ?datumVanVerzenden;
                    prov:generated ?formData .

                ?formData dct:type ?decisionType .

                OPTIONAL {{ ?formData <http://linkedeconomy.org/ontology#financialYear> ?boekjaar . }}

                VALUES ?decisionType {{ {1} }}

                FILTER NOT EXISTS {{ ?inzending nmo:receivedDate ?receivedDate. }}

                OPTIONAL {{ ?formData ext:sessionStartedAtTime ?sessionDate. }}

                BIND(0 AS ?default_attempts)
                OPTIONAL {{ ?inzending ext:failedSendingAttempts ?attempts. }}
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                FILTER(?result_attempts < {0})
            }}
            GRAPH ?h {{
                OPTIONAL {{ ?decisionType skos:prefLabel ?decisionTypeLabel }} .
            }}
        }}
        """.format(max_sending_attempts, separator.join(allowedDecisionTypesList))
    return q


def construct_increment_inzending_attempts_query(graph_uri, inzending_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a certain inzending without succes.

    :param graph_uri: string
    :param inzending_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a meb:Submission .

                OPTIONAL {{ <{1}> ext:failedSendingAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, inzending_uri)
    return q


def construct_inzending_sent_query(graph_uri, inzending_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party (and thus 'sent' by us)

    :param graph_uri: string
    :param bericht_uri: URI of the bericht we would like to mark as sent.
    :param verzonden: ISO-string representation of the datetetime when the message was sent
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1}> nmo:receivedDate "{2}"^^xsd:dateTime .
            }}
        }}
        """.format(graph_uri, inzending_uri, verzonden)
    return q


def construct_create_kalliope_sync_error_query(graph_uri, poststuk_uri, message, error):
    now = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat()
    uuid = helpers.generate_uuid()
    error_uri = "http://data.lblod.info/kalliope-sync-errors/" + uuid
    """
    Construct a SPARQL query for creating a new KalliopeSyncError

    :param graph_uri: string
    :param poststuk_uri: URI of the message that triggered an error, can be None
    :param message: string describing the error
    :param error: error catched by the exception catcher
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pav: <http://purl.org/pav/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{5}> a ext:KalliopeSyncError ;
                    rdfs:label {2} ;
                    ext:errorMessage {3} ;
    """
    if poststuk_uri is not None:
        q += """
                    ext:processedMessage <{1}> ;
             """
    q += """
                    pav:createdOn "{4}"^^xsd:dateTime ;
                    pav:createdBy <http://lblod.data.gift/services/berichtencentrum-sync-with-kalliope-service> .
            }}
        }}
    """
    q = q.format(graph_uri,
                 poststuk_uri,
                 escape_helpers.sparql_escape_string(message),
                 escape_helpers.sparql_escape_string(error),
                 now,
                 error_uri)
    return q


def construct_dossierbehandelaar_exists_query(graph_uri, dossierbehandelaar):
    """
    Construct a query for
    selecting a conversatie based on referentieABB (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param referentieABB: string
    :returns: string containing SPARQL query
    """
    identifier = escape_helpers.sparql_escape_string(dossierbehandelaar['identifier'])

    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT DISTINCT ?dossierbehandelaar
        WHERE {{
            GRAPH <{}> {{
                ?dossierbehandelaar a prov:Association ;
                    schema:identifier {} .
            }}
        }}
        """.format(graph_uri, identifier)
    return q


def construct_insert_dossierbehandelaar_query(graph_uri, bericht):
    """
    Construct a SPARQL query for inserting a new dossierbehandelaar.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :returns: string containing SPARQL query
    """

    uuid = helpers.generate_uuid()
    bericht['dossierbehandelaar']['uuid'] = uuid
    bericht['dossierbehandelaar']['uri'] = "http://data.lblod.info/id/dossierbehandelaars/" + uuid

    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a prov:Association;
                    prov:hadRole <http://data.lblod.info/association-role/249969e6-2bfa-48c2-9a37-3f0b97685a24>;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    adms:identifier "{1[identifier]}";
                    schema:email "{1[email]}".
            }}
        }}
        """.format(graph_uri, bericht['dossierbehandelaar'])
    return q


def construct_link_dossierbehandelaar_query(graph_uri, bericht):
    """
    Construct a SPARQL query for linking a dossierbehandelaar to a bericht.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :returns: string containing SPARQL query
    """

    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> ext:heeftBehandelaar <{2[uri]}> .
            }}
        }}
        """.format(graph_uri, bericht, bericht['dossierbehandelaar'])
    return q


def construct_get_messages_by_status(status_uri, max_confirmation_attempts, bericht_uri=None):
    bound_bericht_statement = ""
    if bericht_uri:
        bound_bericht_statement = "BIND(<{0}> as ?bericht)".format(bericht_uri)

    query_str = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        SELECT DISTINCT ?bericht
                        ?uuid
                        ?verzonden
                        ?ontvangen
                        ?inhoud
                        ?van
                        ?naar
                        ?status
                        ?deliveredAt
                        ?typeCommunicatie
                        ?confirmationAttempts
                        ?g
        {{
            GRAPH ?g {{
                BIND(<{0}> as ?status)
                {1}

                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?uuid;
                    schema:dateSent ?verzonden;
                    schema:dateReceived ?ontvangen;
                    schema:text ?inhoud;
                    schema:sender ?van;
                    schema:recipient ?naar;
                    adms:status ?status;
                    ext:deliveredAt ?deliveredAt;
                    <http://purl.org/dc/terms/type> ?typeCommunicatie.

                OPTIONAL {{ ?bericht ext:failedConfirmationAttempts ?confirmationAttempts. }}
            }}
            FILTER( REGEX(STR(?g), "LoketLB-berichtenGebruiker"))
        }}
    """.format(status_uri, bound_bericht_statement)

    return query_str


def construct_update_bericht_status(bericht_uri, status_uri):
    query_str = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        DELETE {{
          GRAPH ?g {{
             ?bericht adms:status ?status.
          }}
        }}
        INSERT {{
          GRAPH ?g {{
             ?bericht adms:status <{1}>.
          }}
        }}
        WHERE {{
          BIND(<{0}> as ?bericht)

          GRAPH ?g {{
             ?bericht adms:status ?status.
          }}
        }}
    """.format(bericht_uri, status_uri)

    return query_str


def construct_increment_confirmation_attempts_query(graph_uri, poststuk_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a conformation for a certain message without succes.

    :param graph_uri: string
    :param poststuk_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """

    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX schema: <http://schema.org/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedConfirmationAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedConfirmationAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.

                OPTIONAL {{ <{1}> ext:failedConfirmationAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, poststuk_uri)

    return q
