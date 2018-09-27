# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 14:55:28 2018

@author: lapotre
"""

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import json


#data = pd.read_csv('/home/lapotre/articles_blog/inriaviz/sampleSPath.csv')
data = pd.read_csv('~/inriaviz/sampleSPath.csv')


final_dict = {}
sparql = SPARQLWrapper("http://data.bnf.fr/sparql")
def query(entrydict, jointlevelkey, minus, number):
        
    for i in range(len(pd.DataFrame(entrydict)[jointlevelkey].dropna())) :
        if pd.DataFrame(entrydict)[jointlevelkey].dropna()[i]['type'] == 'uri' : 
            sparql.setQuery("""

                        SELECT distinct *
                        WHERE
                        {
                        <%s> ?lien%s%s ?level%s.                
                        }               
                    """%(pd.DataFrame(entrydict)[jointlevelkey].dropna()[i]['value'], minus, number, number))
           
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            entrydict[i]['children'] = results['results']['bindings']
        else :
            entrydict[i]['children'] = []
for uri in data['entite'][0:5] :
   
    sparql.setQuery("""
                    SELECT distinct *
                    WHERE
                    {
                    <%s> ?lien01 ?level1.                
                    }               
                """%uri)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    final_dict[uri] = results['results']['bindings']
    list_df = []
    
    
    query(final_dict[uri], 'level1', '1', '2')
    
    for e in range(len(final_dict[uri])) :
        if len(final_dict[uri][e]['children']) > 0 :
            query(final_dict[uri][e]['children'], 'level2', '2', '3')
            for f in range(len(final_dict[uri][e]['children'])):
                if len(final_dict[uri][e]['children'][f]['children']) > 0 :
                    query(final_dict[uri][e]['children'][f]['children'], 'level3', '3', '4')
                    for g in range(len(final_dict[uri][e]['children'][f]['children'])):
                        if len(final_dict[uri][e]['children'][f]['children'][g]['children']) > 0 :
                            query(final_dict[uri][e]['children'][f]['children'][g]['children'], 'level4', '4', '5')
                            for h in range(len(final_dict[uri][e]['children'][f]['children'][g]['children'])):
                                if len(final_dict[uri][e]['children'][f]['children'][g]['children'][h]['children']) > 0 :
                                    query(final_dict[uri][e]['children'][f]['children'][g]['children'][h]['children'], 'level5', '5', '6')


dump = json.dumps(final_dict)
o = open('~/inriaviz/sampleSPath.json').write()
o.write(dump)
o.close()

#for e in final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'] :
#    query(final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'][e]['children'], 'level2', '2', '3')
          