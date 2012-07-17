#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Pydbcollector is a collection of functions and interfaces to assist the
retrieval of data from the Brazilian Ministry of Education (MEC) online
database of Brazilian Higher Education Institutions (HEI).

Centro de Educação Tecnológica Celso Suckow da Fonseca, 2011-2012.
author: Felipe Aragão Pires.
license: MIT License - http://opensource.org/licenses/MIT

Part of the pydbcollector package.
by @f03lipe, 2011-2012
"""

from tablegenerator import TableGenerator

import psycopg2.extras
import sys

__all__ = ['tablegenerator', 'dataparser', 'queryassembler']

def connect_database(config_file = 'dbconfig.json'):
	""" Return the database cursor.

	This function reads the configuration json file, estabilishes
	the connection to the database and returns a db cursor.
	
	"""
	import json

	with open(config_file) as f:
		data = json.load(f)

	conn = psycopg2.connect(dbname=data['dbname'], user=data['user'], password=data['password'], host=data['host'])
	conn.set_client_encoding('UTF8') # important for the non-ASCII characters
	print "database is now connected. retrieving cursor."
	return conn.cursor()

def display_data(datum):
	for data in datum:
	 	print '\t'.join(map(str, data))

if __name__ == "__main__":
	cursor = connect_database()

	tg = TableGenerator(cursor)
	
	for i in range(1, len(sys.argv)) or '7':
		display_data(getattr(tg, 'table%s' % sys.argv[i])())
	