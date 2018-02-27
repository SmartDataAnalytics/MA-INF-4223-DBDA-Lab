from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore
import io


data_graph = Graph(SPARQLStore("http://dbpedia.org/sparql", context_aware=False))


spheres = ['Language']

query = """
prefix dbpedia-owl: <http://dbpedia.org/ontology/>

select ?entry ?p ?o {{{{
  select ?entry ?p ?o{{ 
    ?entry a dbpedia-owl:{sphere}.
    ?entry ?p ?o
  }}
  order by ?entry
}}}} 

limit {limit}
offset {offset}
"""



data_path = "C:\\Users\\seyit\\Desktop\\BDALab"

total = 1000000
for sphere in spheres:
    i = 0
    with io.open(data_path + "\\{}.txt".format(sphere.lower()), "w", encoding="utf-8") as f:
        while i < total:
            fetch = query.format(sphere = sphere, limit = str(10000), offset = str(i))
            data = data_graph.query(fetch)
            object = None
            for entry in data:
                if 'http://' in entry[2]:
                    object = 'link'
                else:
                    object = 'literal'
                rdf = ('<' + str(entry[0]) + '>' + ' ' 
                + '<' + str(entry[1]) + '>' + ' ' 
                + '<' + str(object) + '>' + '\n')
                f.writelines(rdf)
            i = i + 10001
            print(sphere, i)
    f.close()