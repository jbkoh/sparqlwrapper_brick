import os
import json
import pdb
from copy import deepcopy
import urllib.parse
url_encode = urllib.parse.quote_plus

import rdflib
from rdflib import RDFS, RDF, OWL, Namespace
from rdflib.namespace import FOAF
from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper import JSON, SELECT, INSERT, DIGEST, GET, POST
from rdflib import URIRef, Literal

import pandas as pd

def normalize_uri(s):
    assert isinstance(s, str)
    s = s.replace(' ', '_')
    return url_encode(s)

class BrickEndpoint(object):

    def __init__(self, sparql_url, brick_version, base_ns='', load_schema=True):
        self.BRICK_VERSION = brick_version
        self.sparql_url = sparql_url
        self.sparql = SPARQLWrapper(endpoint=self.sparql_url,
                                    updateEndpoint=self.sparql_url + '-auth')
        self.sparql.queryType= SELECT
        self.sparql.setCredentials('dba', 'dba')
        self.sparql.setHTTPAuth(DIGEST)
        if not base_ns:
            base_ns = 'http://example.com/'
        self.BASE = Namespace(base_ns)
        self.base_graph = base_ns[:-1]
        self.sparql.addDefaultGraph(self.base_graph)
        self.BRICK = Namespace(
            'https://brickschema.org/schema/{0}/Brick#'\
            .format(self.BRICK_VERSION))
        self.BRICK_USE = Namespace(
            'https://brickschema.org/schema/{0}/BrickUse#'\
            .format(self.BRICK_VERSION))
        self.BF = Namespace(
            'https://brickschema.org/schema/{0}/BrickFrame#'\
            .format(self.BRICK_VERSION))
        self.BRICK_TAG = Namespace(
            'https://brickschema.org/schema/{0}/BrickTag#'\
            .format(self.BRICK_VERSION))

        self.namespaces = {
            '': self.BASE,
            'base': self.BASE,
            'brick':self.BRICK,
            'bf': self.BF,
            'brick_tag': self.BRICK_TAG,
            'brick_use': self.BRICK_USE,
            'rdfs': RDFS,
            'rdf': RDF,
            'owl': OWL,
            'foaf': FOAF
        }
        self.q_prefix = ''
        #self.q_prefix = 'DEFINE input:inference <adxrules>\n'
        for prefix, ns in self.namespaces.items():
            if 'uri' in dir(ns):
                ns_n3 = ns.uri.n3()
            else:
                ns_n3 = ns[''].n3()

            self.q_prefix += 'prefix {0}: {1}\n'.format(prefix, ns_n3)
        self.q_prefix += '\n'

        self._init_brick_constants()
        if load_schema:
            self.load_schema()
        self.init_topclasses()

    def add_namespace(self, prefix, ns):
        ns = Namespace(ns)
        self.namespaces[prefix] = ns
        if 'uri' in dir(ns):
            ns_n3 = ns.uri.n3()
        else:
            ns_n3 = ns[''].n3()

        self.q_prefix += 'prefix {0}: {1}\n'.format(prefix, ns_n3)

    def _init_brick_constants(self):
        self.HAS_LOC = URIRef(self.BF + 'hasLocation')

    def _get_sparql(self):
        # If need to optimize accessing sparql object.
        return self.sparql

    def update(self, qstr):
        return self.query(qstr, is_update=True)

    def _format_select_res(self, raw_res):
        var_names = raw_res['head']['vars']
        values = [[row[var_name]['value'] if var_name in row else None
                   for var_name in var_names]
                  for row in raw_res['results']['bindings']]
        var_names = ['?'+var_name for var_name in var_names]
        return [var_names, values]

    def parse_result(self, res):
        raw_res = res
        common_res = res
        return common_res, raw_res

    def raw_query(self, qstr):
        return self.query(qstr)

    def query(self, qstr, is_update=False):
        sparql = self._get_sparql()
        if is_update:
            sparql.setMethod(POST)
        else:
            sparql.setMethod(GET)
        sparql.setReturnFormat(JSON)
        qstr = self.q_prefix + qstr
        sparql.setHTTPAuth
        sparql.setQuery(qstr)
        raw_res = sparql.query().convert()
        if sparql.queryType == SELECT:
            res = self._format_select_res(raw_res)
        elif sparql.queryType == INSERT:
            res = raw_res # TODO: Error handling here
        elif sparql.queryType == 'LOAD':
            res = raw_res # TODO: Error handling here
        return res

    def _create_insert_query(self, triples):
        q = """
            INSERT DATA {{
                GRAPH <{0}> {{
            """.format(self.base_graph)
        for triple in triples:
            triple_str = ' '.join([term.n3() for term in triple]) + ' .\n'
            q += triple_str
        q += """}
            }
            """
        return q

    def _is_bool(self, s):
        s = s.lower()
        if s == 'true' or s == 'false':
            return True
        else:
            return False

    def _str2bool(self, s):
        s = s.lower()
        if s == 'true':
            return True
        elif s == 'false':
            return False
        else:
            raise Exception('{0} is not convertible to boolean'.format(s))

    def _is_float(self, s):
        try:
            float(s)
            return True
        except:
            return False

    def _parse_term(self, term):
        if isinstance(term, URIRef) or isinstance(term, Literal):
            return term
        elif isinstance(term, str):
            if 'http' == term[0:4]:
                node = URIRef(term)
            elif ':' in term: #TODO: This condition is dangerous.
                [ns, id_] = term.split(':')
                ns = self.namespaces[ns]
                node = ns[id_]
            else:
                if term.isdigit():
                    term = int(term)
                elif self._is_float(term):
                    term = float(term)
                elif self._is_bool(term):
                    term = self._str2bool(term)
                else:
                    # Otherwise, just str
                    pass
                node = Literal(term)
        else:
            node = Literal(term)
        return node

    def add_triple(self, pseudo_s, pseudo_p, pseudo_o):
        triple = self.make_triple(pseudo_s, pseudo_p, pseudo_o)
        return self._add_triples([triple])

    def make_triple(self, pseudo_s, pseudo_p, pseudo_o):
        s = self._parse_term(pseudo_s)
        p = self._parse_term(pseudo_p)
        o = self._parse_term(pseudo_o)
        return (s, p, o)

    def add_triples(self, pseudo_triples):
        if not pseudo_triples:
            # TODO: Define the right format same ass _add_triples.
            return True
        triples = [self.make_triple(*pseudo_triple)
                   for pseudo_triple in pseudo_triples]
        return self._add_triples(triples)

    def _add_triples(self, triples):
        q = self._create_insert_query(triples)
        res = self.update(q)
        return res


    def add_brick_instance(self, entity_name, tagset):
        entity = URIRef(self.BASE + entity_name)
        tagset = URIRef(self.BRICK + tagset)
        triples = [(entity, RDF.type, tagset)]
        self._add_triples(triples)
        return str(entity)

    def load_ttlfile(self, filepath):
        q = """
        load <file://{0}> into <{1}>
        """.format(filepath, self.base_graph)
        res = self.update(q)

    def load_schema(self):
        schema_urls = [str(ns)[:-1] + '.ttl' for ns in
                       [self.BRICK, self.BRICK_USE, self.BF, self.BRICK_TAG]]
        load_query_template = 'LOAD <{0}> into <{1}>'
        for schema_url in schema_urls:
            qstr = load_query_template.format(
                schema_url.replace('https', 'http'), self.base_graph)
            res = self.update(qstr)

    def init_topclasses(self, force=False):
        topclasses_file = 'Brick/topclasses.json'
        if os.path.isfile(topclasses_file) and not force:
            with open(topclasses_file, 'r') as fp:
                self.topclasses = json.load(fp)
        else:
            self.topclasses = {}
            target_topclasses = ['Point', 'Equipment', 'Location']
            qstr_template = """
            select ?tagset where {{
            ?tagset rdfs:subClassOf* brick:{0}.
            }}
            """
            for topclass in target_topclasses:
                qstr = qstr_template.format(topclass)
                res = self.query(qstr)
                for [tagset] in res[1]:
                    self.topclasses[tagset.split('#')[-1].lower()] = topclass.lower()
            with open(topclasses_file, 'w') as fp:
                json.dump(self.topclasses, fp, indent=2)

    def sparqlres2df(self, res):
        column_names = res[0]
        data = res[1]
        return pd.DataFrame(data=data, columns=column_names)

    def sparqlres2csv(self, res, filename):
        self.sparqlres2df(res).to_csv(filename)

    def get_top_class(self, tagset):
        pass

    def get_tagset_type(self, tagset):
        pure_tagset = tagset.split('-')[0]
        postfix = pure_tagset.split('_')[-1].lower()
        if postfix in ['server',
                       'networkadapter',
                       ]:
            return 'networkadapter'
        else:
            topclass = self.topclasses.get(pure_tagset, None)
            if topclass:
                return topclass.lower()
            else:
                if postfix in ['sensor',
                               'setpoint',
                               'status',
                               'alarm',
                               'command',]:
                    return 'point'
                else:
                    return 'unidentified'

    def normalize2uri(self, s):
        return '_'.join(re.findall('[a-zA-Z0-9]+', s))

    def serialize_graph(self, filename, nobrick=True):
        g = rdflib.Graph()
        qstr = """
        select ?s ?p ?o where {
        ?s ?p ?o.
        }
        """
        res = self.query(qstr)
        for row in res[1]:
            g.add((URIRef(row[0]), URIRef(row[1]), URIRef(row[2])))
        g.serialize(filename, format='turtle')

    def check_tag_in_tagset(self, tag, tagset):
        TODO



if __name__ == '__main__':
    endpoint = BrickEndpoint('http://localhost:8890/sparql', '1.0.3')
    endpoint.load_schema()
    test_qstr = """
        select ?s where {
        ?s rdfs:subClassOf+ brick:Temperature_Sensor .
        }
        """
    res = endpoint.query(test_qstr)
    print(res)
