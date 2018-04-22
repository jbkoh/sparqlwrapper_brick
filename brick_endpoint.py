import rdflib
from rdflib import RDFS, RDF, OWL
from SPARQLWrapper import SPARQLWrapper, JSON, SELECT, DIGEST
from SPARQLWrapper import JSON, SELECT, DIGEST, GET, POST

from copy import deepcopy

import pdb


class BrickEndpoint(object):
    def __init__(self, sparql_url, brick_version, base_ns=''):
        self.BRICK_VERSION = brick_version
        self.sparql_url = sparql_url
        self.sparql = SPARQLWrapper(endpoint=self.sparql_url,
                                    updateEndpoint=self.sparql_url + '-auth')
        self.sparql.queryType= SELECT
        self.sparql.setCredentials('dba', 'dba')
        self.sparql.setHTTPAuth(DIGEST)
        if not base_ns:
            base_ns = 'http://example.com/'
        self.BASE = base_ns
        self.base_graph = base_ns[:-1]
        self.sparql.addDefaultGraph(self.base_graph)
        self.BRICK = 'https://brickschema.org/schema/{0}/Brick#'\
            .format(self.BRICK_VERSION)
        self.BRICK_USE = 'https://brickschema.org/schema/{0}/BrickUse#'\
            .format(self.BRICK_VERSION)
        self.BF = 'https://brickschema.org/schema/{0}/BrickFrame#'\
            .format(self.BRICK_VERSION)
        self.BRICK_TAG = 'https://brickschema.org/schema/{0}/BrickTag#'\
            .format(self.BRICK_VERSION)

        self.prefix = """
            prefix brick: <{0}>
            prefix bf: <{1}>
            prefix brick-tag: <{2}>
            prefix brick-use: <{3}>
            prefix rdfs: <{4}>
            prefix rdf: <{5}>
            prefix owl: <{6}>
	""".format(self.BRICK, self.BF, self.BRICK_TAG, self.BRICK_USE,
                   str(RDFS), str(RDF), str(OWL))

    def _get_sparql(self):
        # If need to optimize accessing sparql object.
        return self.sparql

    def exec_query(self, qstr, is_update=False):
        sparql = self._get_sparql()
        #if is_update:
            #sparql.setMethod(POST)
        #else:
        #    sparql.setMethod(GET)
        qstr = self.prefix + qstr
        sparql.setHTTPAuth
        sparql.setQuery(qstr)
        sparql.setReturnFormat(JSON)
        res = sparql.query().convert()
        if sparql.queryType == SELECT:
            res = res['results']['bindings']
        return res

    def load_schema(self):
        schema_urls = [ns[:-1] + '.ttl' for ns in
                       [self.BRICK, self.BRICK_USE, self.BF, self.BRICK_TAG]]
        load_query_template = 'LOAD <{0}> into <{1}>'
        for schema_url in schema_urls:
            qstr = load_query_template.format(
                schema_url.replace('https', 'http'), self.base_graph)
            res = self.exec_query(qstr, is_update=True)


if __name__ == '__main__':
    endpoint = BrickEndpoint('http://localhost:8890/sparql', '1.0.3')
    endpoint.load_schema()
    test_qstr = """
        select ?s where {
        ?s rdfs:subClassOf+ brick:Temperature_Sensor .
        }
        """
    res = endpoint.exec_query(test_qstr)