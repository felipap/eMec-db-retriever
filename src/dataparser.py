#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
This module defines the DataParser class.

Part of the pydbcollector package.
by @f03lipe, 2011-2012
"""


class DataParser(object):
	""" Parse data retrieved from server.

	This class consists of static methods used to manipulate data used
	during the retrieval and parsing of data by the table generators.
	The static methods are, in order of definition:
		- _parse_turno(entries) # intern method
		- parse_turno(entries)
		- row_to_dict(entries, key_index)
		- filter(entries, table, keyindex=1, autofill=False)
		- row_to_excel(data)

	"""

	def __init__(self):
		pass

	@staticmethod
	def _parse_turno(entries):

		import re

		d = dict()
		reg = re.compile(ur"([\w\ ]+):(\d*)(?:<br>)?", re.UNICODE)

		for entry in entries:
		# além dos, possivelmente, diversos turnos dentro de uma entrada só,
		# há de se multiplicar esses números pelo número total de cursos que
		# tem como grade de vagas aquela "configuração"
		## Exemplo:
		# (50L, "Matutino:20<br>Vespertino:24")
		## => o número somado será 50*20 para Matutino e 50*24 para Vespertino

			vagas_totais = entry[0]
			for g in reg.findall(entry[1].decode('UTF-8')):
				turno = g[0]
				vagas = int(g[1] or 0)

				if turno not in d: # nova classificação na tabela
					d[turno] = vagas*vagas_totais
				else: d[turno] += vagas*vagas_totais
		
		return d

	@classmethod # parse_turno() needs class to call _parse_turno() 
	def parse_turno(cls, entries):
		""" Parse turno data collected from the database.

		parse_turno() generally takes entries from the database of the form (the whole thing)
		
			entries = [
				(8L, 'Noturno:60'),
				(6L, 'Matutino:80<br>Noturno:60'),
				(2L, 'Integral:40'),
				(10L, 'Noturno:'),
				(1L, 'Noturno:70'),
				(1L, 'Matutino:50'),
				(1L, 'Noturno:')
			]
		
		and returns a dict with the calculations done. for the entry above:
		
			{u'Integral': 80L, u'Noturno': 910L, u'Matutino': 530L}

		when, though, there are secondary "selections", as in

			entries = [
				(8L, 'Noturno:60', 'CEFET'),
				(6L, 'Matutino:80<br>Noturno:60', 'IFET'),
				(2L, 'Integral:40', 'CEFET'),
				(10L, 'Noturno:', 'IFET'),
				(1L, 'Noturno:70', 'IFET'),
				(1L, 'Matutino:50', 'CEFET'),
				(1L, 'Noturno:', 'CEFET')
			]

		the entries are supposed to be grouped by such items, starting
		of by the third element of each entry. the return must be

			{
				'CEFET': {u'Integral': 80L, u'Noturno': 480L, u'Matutino': 50L},
				'IFET': {u'Noturno': 430L, u'Matutino': 480L}
			}

		if the custom selection (everything from third element on) has over one element,
		the return dictionary key will be a tuple 

		""" 

		if not entries:
			return dict()

		if entries[0].__len__() == 2: # for backwards compatibility sake!
			return cls._parse_turno(entries)

		from collections import defaultdict

		res = defaultdict(list)
		for entry in entries:
			res[tuple(entry[2:])].append(entry[:2])

		selected = {}
		for selection in res:
			key = selection if len(selection) > 1 else selection[0]
			selected[key] = cls._parse_turno(res[selection])

		return selected

	@staticmethod
	def row_to_dict(entries, keyindex):
		""" Create a dictionary using item from list of tuples. 

		recieves a list with rows of fetched data
		returns a dictionaries (one entry for each given row) with the element 'key_index' of each promoted as the key
		works for any ordinary dictionary of the kind
		example: given
		
			entries = [
				('A', '1', 'I'),
				('B', '2', 'II'),
				('C', '3', 'III'),
				('D', '4', 'IV'),
			]
		
		and 
		
			key_index = 2
		
		returns

			{
				'I': ('A', '1'),
				'II': ('B', '2'),
				'III': ('C', '3'),
				'IV': ('D', '4'),
			}
		"""
		
		d = dict()
		for row in entries:
			key = row[keyindex]
			assert key not in d, "the data is collapsion \\\\o with key %s." % key
			new_dict[key] = row[:row.index(key)]+row[row.index(key)+1:]
		return new_dict

	@staticmethod
	def filter(entries, table, keyindex=1, autofill=False, row_size=2):
		""" Replace and filter matching keys according to table.

		this takes a key_index and tries to match the [keyindex] element in a row,
		replacing it as specified in 'table'. discarts rows that don't match.
		
		'entries' must be a list of rows. in this case, keyindex = 1.
		eg.:
			(
				('boo', 'Instituto Federal de Educação, Ciência e Tecnologia', ... ),
				('tán', 'Centro Federal de Educação Tecnológica')
				('não', 'UNIVERSIDADE FEDERAL DO PARAN\xc3\x81 - UFPR', ... ),
				('lol', 'Centro Federal de Educação Tecnológica'),
			)

		'table' must be a dictionary with the matching strings as keys and replacing
		strings as values. 
		eg.:
			{
				'Centro Federal de Educação Tecnológica': 'CEFET',
				'Instituto Federal de Educação, Ciência e Tecnologia': 'IFET',
			}

		the returned list shall, then, be
			(
				('boo', 'IFET', ... ),
				('tán', 'CEFET'),
				('lol', 'CEFET'),
			)
	
		!!!!
		rows need not to have the same length, as long as the keyindex-th element always exists.

		"""
		# print entries
		n = []
		if autofill:
			
			# if autofill, get items that are in the translation table but not in
			# the entries, and insert an data-empty row in the returning list of
			# length keyindex+1, with the last element being the not found item and
			# 0 as the remaining.

			for item in table:
				# if key item is not in any entry
				if not item in [e[keyindex] for e in entries]:
		
					if entries:
						# if entries is not [], l will have the same length as the
						# lengthiest row within entries. taking the smallest row's
						# length would probably work just as well. why, after all,
						# would two entries have different lengths?
						l = [autofill]*max(len(e) for e in entries)
					else:
						# if entries is empty, build rows of size 'row_size'.
						if not row_size:
							# if variable is null, build rows of size keyindex+1.
							row_size = keyindex+1
						# the row_size variable must be passed in those table
						# generators that fail with entries of size keyindex+1!
						# (dahh)
						l = [autofill]*row_size

					l[keyindex] = table[item]
					n.append(l)

		for entry in entries:
			entry = list(entry) # allow index assigning
			if entry[keyindex] in table:
				entry[keyindex] = table[entry[keyindex]]
				n.append(entry)

		# sort based on keyindex. this is crucial!
		return sorted(n, key = lambda row: row[keyindex])