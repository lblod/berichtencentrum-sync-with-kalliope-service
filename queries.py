#!/usr/bin/python3
import copy
import escape_helpers

# TODO Remove this escape method and use the mu one once PR has been merged: https://github.com/MikiDi/mu-python-template/pull/2
import re
def sparql_escape_string(obj):
    obj = str(obj)
    def replacer(a):
        return "\\"+a.group(0)
    return '"""' + re.sub(r'[\\\'"]', replacer, obj) + '"""'


def construct_conversatie_exists_query(graph_uri, dossiernummer):
    """
    Construct a query for selecting a conversatie based on dossiernummer (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param dossiernummer: string
    :returns: string containing SPARQL query
    """
    dossiernummer = escape_helpers.sparql_escape_string(dossiernummer)
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie
        WHERE {{
            GRAPH <{}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier {}.
            }}
        }}
        """.format(graph_uri, dossiernummer)
    return q

def construct_bericht_exists_query(graph_uri, bericht_uri):
    """
    Construct a query for selecting a bericht based on its URI, retrieving the conversatie & dossiernummer at the same time.

    :param graph_uri: string
    :param bericht_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie ?dossiernummer
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                ?conversatie a schema:Conversation;
                    schema:hasPart <{1}>;
                    schema:identifier ?dossiernummer.
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q

def construct_insert_conversatie_query(graph_uri, conversatie, bericht):
    """
    Construct a SPARQL query for inserting a new conversatie with a first bericht attached.

    :param graph_uri: string
    :param conversatie: dict containing escaped properties for conversatie
    :param bericht: dict containing escaped properties for bericht
    :returns: string containing SPARQL query
    """
    conversatie = copy.deepcopy(conversatie) # For not modifying the pass-by-name original
    conversatie['dossiernummer'] = escape_helpers.sparql_escape_string(conversatie['dossiernummer'])
    conversatie['betreft'] = escape_helpers.sparql_escape_string(conversatie['betreft'])
    conversatie['current_type_communicatie'] = escape_helpers.sparql_escape_string(conversatie['current_type_communicatie'])
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier {1[dossiernummer]};
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
                    schema:text {2[inhoud]};
                    <http://purl.org/dc/terms/type> "{2[type_communicatie]}";
                    schema:sender <{2[van]}>;
                    schema:recipient <{2[naar]}>.
            }}
        }}
    """
    q = q.format(graph_uri, conversatie, bericht)
    return q

def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :returns: string containing SPARQL query
    """
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{2}> a schema:Conversation;
                    schema:hasPart <{1[uri]}>.
                <{1[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:dateSent "{1[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{1[ontvangen]}"^^xsd:dateTime;
                    schema:text {1[inhoud]};
                    schema:sender <{1[van]}>;
                    schema:recipient <{1[naar]}>;
                    <http://purl.org/dc/terms/type> "{1[type_communicatie]}".
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri)
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

def construct_insert_bijlage_query(bericht_graph_uri, bijlage_graph_uri, bericht_uri, bijlage, file):
    """
    Construct a SPARQL query for inserting a bijlage and attaching it to an existing bericht.

    :param bericht_graph_uri: string
    :param bijlage_graph_uri: string
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
                <{2}> nie:hasPart <{3[uri]}>.
            }}
            GRAPH <{1}> {{
                <{3[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{3[uuid]}";
                    nfo:fileName {3[name]};
                    dct:format {3[mimetype]};
                    dct:created "{3[created]}"^^xsd:dateTime;
                    nfo:fileSize "{3[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{3[extension]}".
                <{4[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{4[uuid]}";
                    nfo:fileName {4[name]};
                    dct:format {3[mimetype]};
                    dct:created "{3[created]}"^^xsd:dateTime;
                    nfo:fileSize "{3[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{3[extension]}";
                    nie:dataSource <{3[uri]}>.
            }}
        }}
        """.format(bericht_graph_uri, bijlage_graph_uri, bericht_uri, bijlage, file)
    return q

def construct_update_last_bericht_query_part1():
    """
    Construct a SPARQL query for keeping the ext:lastMessage of each conversation up to date.
    Part 1/2 (query constructed in 2 parts because Virtuoso)

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {
            GRAPH ?g {
                ?conversation ext:lastMessage ?message.
            }
        }
        WHERE {
            GRAPH ?g {
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage;
                    ext:lastMessage ?message.
            }
            {
                SELECT (?message AS ?newMessage) WHERE {
                    GRAPH ?g {
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent < ?otherDateSent  )
                        }
                    }
                }
            }
        }
        """
    return q

def construct_update_last_bericht_query_part2():
    """
    Construct a SPARQL query for keeping the ext:lastMessage of each conversation up to date.
    Part 2/2 (query constructed in 2 parts because Virtuoso)

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT {
            GRAPH ?g {
                ?conversation ext:lastMessage ?newMessage.
            }
        }
        WHERE {
            GRAPH ?g {
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage.
            }
            {
                SELECT (?message AS ?newMessage) WHERE {
                    GRAPH ?g {
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent < ?otherDateSent  )
                        }
                    }
                }
            }
        }
        """
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

        SELECT DISTINCT ?dossiernummer ?dossieruri ?bericht ?betreft ?uuid ?van ?verzonden ?inhoud
        WHERE {{
            GRAPH ?g {{
                ?conversatie a schema:Conversation;
                    schema:identifier ?dossiernummer;
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
        }}
        """.format(naar_uri, max_sending_attempts)
    return q

def construct_select_bijlagen_query(bijlagen_graph_uri, bericht_uri):
    """
    Construct a SPARQL query for retrieving all bijlages for a given bericht.

    :param bijlagen_graph_uri: string, graph where file information is stored
    :param bericht_uri: URI of the bericht for which we want to retrieve bijlagen.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT ?bijlagenaam ?file ?type WHERE {{
            GRAPH ?g {{
                <{1}> a schema:Message;
                    nie:hasPart ?bijlage.
            }}
            GRAPH <{0}> {{
                ?bijlage a nfo:FileDataObject;
                    nfo:fileName ?bijlagenaam;
                    dct:format ?type.
                ?file nie:dataSource ?bijlage.
            }}
        }}
        """.format(bijlagen_graph_uri, bericht_uri)
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

        SELECT ?origineelbericht WHERE {{
            GRAPH ?g {{
                ?conversation a schema:Conversation;
                    schema:hasPart ?origineelbericht;
                    schema:hasPart <{0}>.
            }}
            {{
                SELECT (?message AS ?origineelbericht) WHERE {{
                    GRAPH ?g {{
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {{
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent > ?otherDateSent )
                        }}
                    }}
                }}
            }}
        }}
        """.format(bericht_uri)
    return q

def construct_unsent_inzendingen_query(max_sending_attempts):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param max_sending_attempts: the maximum number of delivery attempts that have to be done
    :returns: string containing SPARQL query
    """

    allowedDecisionTypesList = [
        '<http://data.lblod.info/DecisionType/9ce1fbfa68b14a5599b266f0e0211bf34b6be9d3792e4ee4a893de6525bd1331>',
        '<http://data.lblod.info/DecisionType/80536574a0ec8ea88685510b713aa566a5f16cfd575fabd8f7943bccaaad00e4>',
        '<http://data.lblod.info/DecisionType/4c7e8ce005e798d980ad0b7548b55a461cb6983c18eafbbb5e5cae617aae2e3d>',
        '<http://data.lblod.info/DecisionType/26697366c439cac0fd35581416baffec2368d765d61888bfb4bafd22ddbc8b33>',
        '<http://data.lblod.info/DecisionType/d6e90eb6e3ceda4f9a47b214b3ab47274670d3621f34bf8984f4c7d99f97dcc2>',
        '<http://data.lblod.info/DecisionType/a6f91929f3d9625991863c2fec4f6a4fe4287753eaefb901faaaa61002ba378a>',
        '<http://data.lblod.info/DecisionType/c52ff4c14694c22c55dcf01b3f30b5dc00bc8b264260488b2d14de80953964be>',
        '<http://data.lblod.info/DecisionType/e95eb08daa9892357e52914596cf77945c4a3086d850344ed17c80c96b9686a7>',
        '<http://data.lblod.info/DecisionType/898539285cb768813b1078651adc0c31c057423dbf421e254c5ded013a436284>',
        '<http://data.lblod.info/DecisionType/1c0a7dae2be26ee48a31ca80508fbc6defb046791655283b5b91ee0d5242e675>',
        '<http://data.lblod.info/DecisionType/d68c36802386c988df798ed577b96d8e0f010441f74628f4d2dbd3196c1c6ac3>',
        '<http://data.lblod.info/DecisionType/ce6b083256a2cda03c31f71e412747f079fc956ddeb2fa954725807e3fa03ea3>',
        '<http://data.lblod.info/DecisionType/072413568f428f8490f55729f8e577979fcb9a28c4fc0727fbf539677bef1dce>',
        '<http://data.lblod.info/DecisionType/f1befb94c7f074a34d1e2b594141e6e53ce63b7c65a349885d1026779df581ed>',
        '<http://data.lblod.info/DecisionType/e9d17bc6fb58ea6049d702294672e135d4952963d938c665eaa7cee3817f1c06>',
        '<http://data.lblod.info/DecisionType/6921c32162d2340fc4808e2b3e2ac4164d2fd53d9d4a722a894015fd2a559588>',
        '<http://data.lblod.info/DecisionType/31174a1b9ba5400d8ccb6d3ecad0cf43372b54a362e1ae9abf597a67120119ca>',
        '<http://data.lblod.info/DecisionType/14fbb6a2cc518a9e116835e3c9c3bdd858feac647e29334e404ac5a2dfa80ccb>',
        '<http://data.lblod.info/DecisionType/8ff851da2264fc1d6afb57ac23c7a4d492a509870674dd419572048ecdd63b5d>',
        '<http://data.lblod.info/DecisionType/600799c1f8e1a64b370b5f16507d0a8e76dae56118340fcae7d4887ee46bdbbd>',
        '<http://data.lblod.info/DecisionType/04e8ea609fa954e1e3324afee57c358f8cb324e4b57277b54f1734c331922f5e>',
        '<http://data.lblod.info/DecisionType/9fb28f4a126a4f02940408344c37b81c32f567c30a7b0390dee14e8ead197b64>',
        '<http://data.lblod.info/DecisionType/012ee8325a88bf82c32b06bea1a0c54c6abf116304daa5bdd82b0c9f910a9c41>',
        '<http://data.lblod.info/DecisionType/3814e7380c3823eb7cd6d835738ae96382e1b76e82acca848f9055d922f84f3c>',
        '<http://data.lblod.info/DecisionType/fe25499216a96fc3da8ef79294acdf64f0fb838ef388f46ea6036b8e7eb6545c>',
        '<http://data.lblod.info/DecisionType/36979ac1d57b103ec10732be09705e0183863c3db9726b6352344500e392f33c>'
    ]
    separator = ', '

    q = """
        PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT ?inzending ?inzendingUuid ?bestuurseenheid ?decisionType ?sessionDate ?decisionTypeLabel
        WHERE {{
            GRAPH ?g {{
                ?inzending a toezicht:InzendingVoorToezicht ;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?inzendingUuid ;
                    dct:subject ?bestuurseenheid ;
                    adms:status <http://data.lblod.info/document-statuses/verstuurd> ;
                    toezicht:decisionType ?decisionType ;
                    nmo:sentDate ?sentDate .

                FILTER ( ?decisionType IN ( {1} ) )

                FILTER NOT EXISTS {{ ?inzending nmo:receivedDate ?receivedDate. }}

                OPTIONAL {{ ?inzending toezicht:sessionDate ?sessionDate. }}

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
        PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
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
                <{1}> a toezicht:InzendingVoorToezicht.
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
        PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>

        INSERT {{
            GRAPH <{0}> {{
                <{1}> nmo:receivedDate "{2}"^^xsd:dateTime .
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a toezicht:InzendingVoorToezicht .
            }}
        }}
        """.format(graph_uri, inzending_uri, verzonden)
    return q
