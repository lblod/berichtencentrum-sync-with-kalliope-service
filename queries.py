#!/usr/bin/python3
import copy
import escape_helpers

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
    conversatie['type_communicatie'] = escape_helpers.sparql_escape_string(conversatie['type_communicatie'])
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = escape_helpers.sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier {1[dossiernummer]};
                    ext:dossierUri "{1[dossierUri]}";
                    schema:about {1[betreft]};
                    <http://purl.org/dc/terms/type> {1[type_communicatie]};
                    schema:processingTime "{1[reactietermijn]}";
                    schema:hasPart <{2[uri]}>.

                <{2[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    schema:dateSent "{2[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{2[ontvangen]}"^^xsd:dateTime;
                    schema:text {2[inhoud]};
                    schema:sender <{2[van]}>;
                    schema:recipient <{2[naar]}>.
            }}
        }}
        """.format(graph_uri, conversatie, bericht)
    return q

def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing escaped properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :returns: string containing SPARQL query
    """
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = escape_helpers.sparql_escape_string(bericht['inhoud'])
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
                    schema:recipient <{1[naar]}>.
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri)
    return q

def construct_insert_bijlage_query(bericht_graph_uri, bijlage_graph_uri, bericht_uri, bijlage, file):
    """
    Construct a SPARQL query for inserting a bijlage and attaching it to an existing bericht.

    :param graph_uri: string
    :param bericht: dict containing escaped properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
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

def construct_unsent_berichten_query(naar_uri):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param graph_uri: string
    :param naar_uri: URI of the recpient for which we want to retrieve messages that have yet to be sent.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        SELECT DISTINCT ?dossiernummer ?dossieruri ?bericht ?betreft ?uuid ?van ?verzonden ?inhoud
        WHERE {{
            GRAPH ?g {{
                ?conversatie a schema:Conversation;
                    ext:dossierUri ?dossieruri;
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
            }}
        }}
        """.format(naar_uri)
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

def construct_bericht_sent_query(graph_uri, bericht_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party.

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
