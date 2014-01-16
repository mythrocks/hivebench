#!/usr/bin/python

import sys, os
from itertools import islice
from string import Template

inFileName = sys.argv[1]
chunkSize = int(sys.argv[2])
query = sys.argv[3]
#fromClause = sys.argv[3]
#selectClause = sys.argv[4]
print 'inFileName: ', inFileName
print 'chunkSize: ', chunkSize
print 'query: ', query
#print 'fromClause: ', fromClause
#print 'selectClause: ', selectClause

template = Template(query)

with open( inFileName, 'r') as inFile:
        lines_gen = islice(inFile, chunkSize)
        lines = list(lines_gen)


        while len(lines) != 0:
		first = lines[0].strip()
		for line in lines:
			second = line.strip()
		finalQuery = template.substitute(replaceme1=first, replaceme2=second)
		print 'Query: ', finalQuery 
		os.system( 'hive -e \"' + finalQuery + '\"' )
                lines_gen = islice(inFile, chunkSize)
                lines = list(lines_gen)

#        while len(lines) != 0:
#                finalSelect = ''
#                for date in lines:
#                        date=date.strip()
#                        finalSelect += '\n' + selectTemplate.substitute(replaceme=date)
#                print 'Final insert statement:\n\n', fromClause + finalSelect
#                os.system( 'hive -e \"' + fromClause + finalSelect + '\"' )
#                lines_gen = islice(inFile, chunkSize)
#                lines = list(lines_gen)
#
