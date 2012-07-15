#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import psycopg2.extras
# from time import sleep

DBNAME = 'robo_mec'
USER = 'robomec'
PASSWORD = 'cefetmec2011!'
HOST = 'mp4-5b.dyndns.info'


def connect_database():
	""" faz conexão com o banco de dados e retorna cursor """

	conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
	conn.set_client_encoding('UTF8')
	print "database is now connected. retrieving cursor."

	return conn, conn.cursor()

# natureza_jurídica: nat_jur

class QueryCreator(object):
	# this is an (attempt to) a query creator

	# the select fields might better be organized as unique unities
	## (don't allow creation of select with multiple parts)
	SELECT = {
		'qtde_de_cursos':
			"", # already added by default
		'nat_juridica':
			"n.natureza_juridica as nat_juridica",
		'org_acad':
			"o.organizacao_academica as org_acad",
		'estado':
			"i.uf as estado",
		'modalidade':
			"m.nome as modalidade",
		'vagas_turno':
			"c.vagas_totais_anuais as vagas_turno"
	}

	# (exclusive) mean that they, by default, don't work with other possibilities
	WHERE = { # some parts of the WHERE field (add explanation)
		'base': # the basic condition that holds all tables tight
			"i.natid = n.id and i.instid = c.instid and c.modid = m.id and c.titid = t.id and i.orgid = o.id",

		'tecnológico':
			"t.nome = 'Tecnológico'",

		'licenciatura':
			"t.nome = 'Licenciatura'",

		'ensino_público': # selects the public schools (exclusive)
			"""((n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Municipal') or (n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Estadual') or
				(n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Federal'))""",
		'ensino_privado': # selects prive schools (exclusive)
			"""((n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Associação de Utilidade Pública') or (n.natureza_juridica = 'Privada sem fins lucrativos')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Sociedade') or (n.natureza_juridica = 'Privada com fins lucrativos')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Mercantil ou Comercial')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Associação de Utilidade Pública'))""",


		'data de início': # selects the date the course started (use basic string formating '%')
			"(SUBSTRING(c.data_inicio FROM 7 FOR 10) < '%s') and not (SUBSTRING(c.data_inicio FROM 7 FOR 10) = '') and not ((SUBSTRING(c.data_inicio FROM 7 FOR 10) = '1010'))", # remove latest special case


		'universidade_federal': # seleciona universidade federal
			# "(SUBSTRING(i.nome FROM 1 FOR 20) = 'UNIVERSIDADE FEDERAL')",
			"(i.nome LIKE '%UNIVERSIDADE FEDERAL%')",
		"utf":
			"(SUBSTRING(i.nome FROM 1 FOR 32) = 'UNIVERSIDADE TECNOLÓGICA FEDERAL')",


		'no norte':
			"(i.uf = 'AM' or i.uf = 'AC' or i.uf = 'PA' or i.uf = 'RO' or i.uf = 'RR' or i.uf = 'TO' or i.uf = 'AP')",
		'no sul':
			"(i.uf = 'SC' or i.uf = 'PR' or i.uf = 'RS')",
		'no nordeste':
			"(i.uf = 'AL' or i.uf = 'BA' or i.uf = 'CE' or i.uf = 'MA' or i.uf = 'PB' or i.uf = 'PE' or i.uf = 'PI' or i.uf = 'RN' or i.uf = 'SE')",
		'no centro-oeste':
			"(i.uf = 'GO' or i.uf = 'MT' or i.uf = 'MS' or i.uf = 'DF')",
		'no sudeste':
			"(i.uf = 'RJ' or i.uf = 'ES' or i.uf = 'SP' or i.uf = 'MG')",

		'educação presencial':
			"(m.nome = 'Educação Presencial')",

		'educação a distância':
			"(m.nome = 'Educação a Distância')",
	}


	def __init__(self, ano, select, where=None, group_by=None, order_by=None):
		
		# this here is a huge mess!!  \\O
		#                              /
		#                             /\

		self.query = ""
		self.ano = ano

		self.add_select(select)
		self.add_from()
		self.add_where(where)
		self.add_group_by(group_by)
		self.add_order_by(order_by) # try to prevent errors from unqualified order_by fields?

	def add_to_query(self, string):
		# Adds a string To the QUERY

		try: self.query
		except AttributeError:
			raise Exception("query variable should exist (or shouldn't it?)")

		if not self.query.endswith(" "):
			self.query += " " # add space just in case
		self.query += (string + " ")
	
	def add_select(self, conditions):
		
		self.add_to_query("SELECT count (*) as qtde_cursos")
		if conditions and conditions != ['qtde_de_cursos']:
			for c in conditions:
				self.add_to_query(', '+self.SELECT[c])
	
	def add_from(self):
		# default FROM field
		self.add_to_query("\n")
		self.add_to_query("FROM curso c, instituicao i, titulacao t, modalidade m, organizacao o, natureza n")

	def add_where(self, conditions):
		# add basic WHERE conditions and additional ones entered as string

		if not conditions:
			raise Exception("esqueceu a condição?")

		conditions = conditions or ['ensino privado', 'data de início']
		if not 'base' in conditions: # melhor 'base' ser adicionada pelo programa
			conditions.insert(0, 'base')

		self.add_to_query("\n")
		self.add_to_query("WHERE")
		for i in range(len(conditions)):
			if conditions[i] == "data de início":
				self.add_to_query(self.WHERE['data de início'] % str(self.ano+1))
			else:
				self.add_to_query(self.WHERE[conditions[i]])
			# if actual isn't the last element to be inserted into the WHERE field
			if i < len(conditions)-1: 
				self.add_to_query("and")

	def add_group_by(self, clauses):
		if clauses:
			self.add_to_query("\n")
			self.add_to_query("group by")
			self.add_to_query(', '.join(clauses))

	def add_order_by(self, clauses):
		if clauses:
			self.add_to_query("\n")
			self.add_to_query("order by")
			self.add_to_query(', '.join(clauses))


class EntriesParser(object):
	# classe para trabalhar os dados recebidos pelas consultas de maneira geral

	def __init__(self):
		pass


	@staticmethod
	def _parse_turno(entries):
		# documentar entrada esperada (TODO)

		import re

		d = dict()
		reg = re.compile(ur"([\w\ ]+):(\d*)(?:<br>)?", re.UNICODE)

		for entry in entries:
		# além dos possivelmente diversos turnos dentro de uma entrada só,
		# há de se multiplicar esses números pelo número total de cursos que
		# tem como grade de vagas aquela "configuração"
		## Exemplo:
		# (50L, "Matutino:20<br>Vespertino:24")
		## => o número somado será 50*20 para Matutino e 50*24 para Vespertino

			vagas_totais = entry[0]
			for g in reg.findall(entry[1].decode('utf-8')):
				turno = g[0]
				vagas = int(g[1] or 0)

				if turno not in d: # nova classificação na tabela
					d[turno] = vagas*vagas_totais
				else: d[turno] += vagas*vagas_totais
		
		return d

	@staticmethod
	def parse_turno(entries):

		"""
		# (trying my best to make it clear)
		# parse_turno() generally takes entries from the database of the form (the whole thing)
		
			entries = [
				(8L, 'Noturno:60'),
				(6L, 'Matutino:80<br>Noturno:60'),
				(2L, 'Integral:40'),
				(10L, 'Noturno:'),
				(1L, 'Noturno:70'),
				(1L, 'Matutino:50'),
				(1L, 'Noturno:')
			]
		
		# and returns a dict with the calculations done. for the entry above:
		
			{u'Integral': 80L, u'Noturno': 910L, u'Matutino': 530L}

		# when, though, there are secondary "selections", as in

			entries = [
				(8L, 'Noturno:60', 'CEFET'),
				(6L, 'Matutino:80<br>Noturno:60', 'IFET'),
				(2L, 'Integral:40', 'CEFET'),
				(10L, 'Noturno:', 'IFET'),
				(1L, 'Noturno:70', 'IFET'),
				(1L, 'Matutino:50', 'CEFET'),
				(1L, 'Noturno:', 'CEFET')
			]

		# the entries are supposed to be grouped by such items,
		# starting of by the third element of each entry.
		# the return should be

			{
				'CEFET': {u'Integral': 80L, u'Noturno': 480L, u'Matutino': 50L},
				'IFET': {u'Noturno': 430L, u'Matutino': 480L}
			}

		# if the custom selection (everything from third element on) has over one element,
		# the return dictionary key will be a tuple 

		""" 

		if not entries:
			return dict()

		if entries[0].__len__() == 2: # for backwards compatibility sake!
			return EntriesParser._parse_turno(entries)

		from collections import defaultdict

		res = defaultdict(list)
		for entry in entries:
			res[entry[2:]].append(entry[:2])

		selected = {}
		for selection in res:
			key = selection if len(selection) > 1 else selection[0]
			selected[key] = EntriesParser._parse_turno(res[selection])

		return selected


	@staticmethod
	def turn_into_dict(entries, key_index):
		"""
		# recieves a list with rows of fetched data
		# returns a dictionaries (one entry for each given row) with the element 'key_index' of each promoted as the key
		# works for any ordinary dictionary of the kind

		# example: given
			entries = [
				('A', '1', 'I'),
				('B', '2', 'II'),
				('C', '3', 'III'),
				('D', '4', 'IV'),
			]
		# and 
			key_index = 2
		# returns
		{
			'I': ('A', '1'),
			'II': ('B', '2'),
			'III': ('C', '3'),
			'IV': ('D', '4'),
		}
		"""
		
		new_dict = dict()
		for row in entries:
			print row
			key = row[key_index]
			print key
			if key in new_dict:
				raise Exception("the data is collapsion \\\\o with key %s." % key)
			new_dict[key] = row[:row.index(key)]+row[row.index(key)+1:]

		return new_dict


	@staticmethod
	def filter_entries(entries, wanted, warning=False, fill=True):

		"""
		# filters the wanted institutions from 'entries' and translated them into the given labels
		# THE INSTITUTIONS MUST BE THE SECOND INFORMATION IN THE FETCHED DATA ROW!!!!
		# returns a dictionary with the wanted information in the the desired label
		
		'wanted' consists of a dictionary with entries of the form {<institution_name>: <label of return>}
		for the wanted institutions to be filtered and translated in the given labels in the returned dictionary.
		e.g.:
			{
				'Centro Federal de Educação Tecnológica': 'CEFET',
				'Instituto Federal de Educação, Ciência e Tecnologia': 'IFET',
				...
			}

	
		'entries' must be the default result rows for the queries:
		e.g.
			(
				(1L, 'UNIVERSIDADE FEDERAL FLUMINENSE - UFF'),
				(2L, 'UNIVERSIDADE FEDERAL DO PARAN\xc3\x81 - UFPR'),
				(1L, 'UNIVERSIDADE FEDERAL DE S\xc3\x83O PAULO - UNIFESP'),
				(1L, 'UNIVERSIDADE FEDERAL DO ESP\xc3\x8dRITO SANTO - UFES')
			)

		if 'warning':
			raises an Exception when institutions in 'wanted' are not found in entries.

		if 'fill':
			fills the wanted entries previously (with 0s)
		"""
		
		# check if all institutions in 'wanted' are present in the entries
		if warning and not set(wanted.keys()).issubset(set((e[1] for e in entries))):
			dif = set(wanted.keys()) - set((e[1] for e in entries))
			raise Exception("some institutions in wanted are not found in entries: %s not found in %s" % (dif, entries))
		
		filtered = dict()
		if fill:
			for w in wanted:
				filtered[wanted[w]] = 0

		if not wanted:
			return entries

		for entry in entries:
			data = entry[0]
			institution = entry[1]

			if institution in wanted:
				label = wanted[institution]
				filtered[label] = data
		
		return filtered


	@staticmethod
	def row_to_excel(data):
		return '\t'.join(map(str, data))


class _tg_metaclass(type):
	"""
	TableGenerator MetaClass
	"""

	def __new__(self, name, parents, attributes):
		"""
		decorates the table generating functions.
		"""

		from types import FunctionType
		from functools import wraps

		mod_attr = attributes.copy()
		
		for name, value in mod_attr.items():
			# check if its a table generating method
			if name.startswith('table') and isinstance(value, FunctionType):
				
				@wraps(value)
				def decorator(method_name, method):
					def gen_method(self, *args, **kwargs):
						# print "gerando %sª tabela" % method_name[5:]
						print method.__doc__.strip()
						return method(self, *args, **kwargs)
					return gen_method
				mod_attr[name] = decorator(name, value) # replace by decorator
		
		return type(name, parents, mod_attr)


class TableGenerator(object):
	# implement methods as generators (sweet) - DONE?
	# add custom description to each table.

	__metaclass__ = _tg_metaclass

	def __init__(self, cursor):
		self.cursor = cursor

	def table1(self):
		"""
		gerando 1ª tabela.
		CST presencial e à distância, segundo a natureza jurídica.
		"""

		mquery = lambda ano, cat_ensino, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos'], where=['tecnológico', cat_ensino, mod_ensino, 'data de início']).query

		# print "-"*40, "query example"
		# print mquery(2002, 'educação presencial')
		# print "-"*40

		for mod_ensino in MOD_ENSINO:
			for cat_ensino in CATEGORIAS_ENSINO:
				print "=> categoria de ensino: %s" % cat_ensino
				print "=> modalidade de ensino: %s" % mod_ensino
				for year in ANOS_FHC+ANOS_LULA:
					cursor.execute(mquery(year, cat_ensino, mod_ensino))
					fetched_data = cursor.fetchall()
					yield EntriesParser.row_to_excel(fetched_data)
				print "#"*40 


	def table2(self):
		"""
		gerando 2ª tabela.
		CST presencial e à distância, segundo a categoria administativa (municipal, estadual e federal).
		"""

		mquery = lambda ano, mod_ensino: QueryCreator(ano, select=['qtde_de_cursos', "nat_juridica"], where=['tecnológico', 'ensino_público', 'data de início', mod_ensino],
				group_by=["nat_juridica"], order_by=["nat_juridica"]).query
		
		# print "-"*40, "query example"
		# print mquery(2002, 'educação presencial')
		# print "-"*40
		

		cursor = self.cursor

		for mod_ensino in MOD_ENSINO:
			print "=> modalidade de ensino: %s" % mod_ensino
			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, mod_ensino))
				fetched_data = EntriesParser.filter_entries(cursor.fetchall(), {
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Municipal": "Municipal",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Estadual": "Estadual",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Federal": "Federal"
				})
				# yield year, fetched_data
				yield EntriesParser.row_to_excel(fetched_data.values())
				
			print "last fetched: %s" % fetched_data
			print "#"*40
	

	def table3(self):
		"""
		gerando 3ª tabela.
		CST presencial e à distância, na rede pública federal, segundo a organização acadêmica (municipal, estadual e federal).
		"""

		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', mod_ensino], order_by=["org_acad"], group_by=["org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs), dado para ser adicionado à fetched_data
		mquery2 = lambda ano, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', 'universidade_federal', mod_ensino], group_by=["org_acad"]).query
		
		# mquery3 cria query específica para UTFs, dado para ser adicionado à fetched_data
		mquery3 = lambda ano, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', 'utf', mod_ensino], group_by=["org_acad"]).query

		# print "-"*40, "query example"
		# print mquery(2002, MOD_ENSINO[1])
		# print "-"*40, '\n'

		for mod_ensino in MOD_ENSINO:
			print "ensino: ", mod_ensino

			for year in ANOS_FHC+ANOS_LULA:

				fetched_data = dict()
				cursor.execute(mquery(year, mod_ensino))
				fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
					"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
					"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
				}))
				
				cursor.execute(mquery2(year, mod_ensino))
				fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
					'Universidade': 'UF',
				}))

				cursor.execute(mquery3(year, mod_ensino))
				fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
					'Universidade': 'UTF'
				}))
				
				yield year, fetched_data

			print "#################"


	def table4(self):
		"""
		gerando 4ª tabela.
		CST presencial e à distância, na rede pública federal, segundo a organização acadêmica (municipal, estadual e federal), por regiões.
		(3ª tabela dividida por regiões)
		"""

		# generates table 4 (tabela 3 separada em regiões)
		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, regiao, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', regiao, mod_ensino], order_by=["org_acad"], group_by=["org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs), dado para ser adicionado à fetched_data
		mquery2 = lambda ano, regiao, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', 'universidade_federal', regiao, mod_ensino], group_by=["org_acad"]).query
		
		# mquery3 cria query específica para UTFs, dado para ser adicionado à fetched_data
		mquery3 = lambda ano, regiao, mod_ensino: QueryCreator(ano=ano, select=['qtde_de_cursos', "org_acad"], where=['tecnológico', 'data de início', 'utf', regiao, mod_ensino], group_by=["org_acad"]).query


		# print "-"*40, "query example"
		# print mquery(2002, REGIOES[0], MOD_ENSINO[0])
		# print "-"*40, '\n'

		cursor = self.cursor

		for mod_ensino in MOD_ENSINO:
			print "ensino: ", mod_ensino
			for regiao in REGIOES:
				print "\tregiao: ", regiao
				for year in ANOS_FHC+ANOS_LULA:

					fetched_data = dict()
					cursor.execute(mquery(year, regiao, mod_ensino))
					fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
						"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
						"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
					}))
					
					cursor.execute(mquery2(year, regiao, mod_ensino))
					fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
						'Universidade': 'UF',
					}))

					cursor.execute(mquery3(year, regiao, mod_ensino))
					fetched_data.update(EntriesParser.filter_entries(cursor.fetchall(), {
						'Universidade': 'UTF'
					}))
					
					# yield year, fetched_data
					yield EntriesParser.row_to_excel(fetched_data.values())
				
				print "last fetched: %s" % fetched_data
				print "#"*30
			print "#"*50+'\n'+"#"*50

	
	def table5_6(self):
		"""
		gerando 5ª e 6ª tabela.
		CST presenciais, em Instituições de Ensino Superior (IES) públicas e privadas, segundo o número de vagas por turno.
		"""

		mquery = lambda ano, cat_ensino: QueryCreator(ano=ano,
			select=['qtde_de_cursos', 'vagas_turno'],
			where=['tecnológico', cat_ensino, 'data de início'], group_by=["vagas_turno"]).query

		# print "-"*40, "query example"
		# print mquery(2002, CATEGORIAS_ENSINO[0])
		# print "-"*40, '\n'

		for cat_ensino in CATEGORIAS_ENSINO:
			print "categoria de ensino: %s" % cat_ensino

			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, cat_ensino))				
				fetched_data = EntriesParser.parse_turno(cursor.fetchall()).items()
				fetched_data = [(b,a) for a,b in fetched_data]

				fetched_data = EntriesParser.filter_entries(fetched_data, {
					u'Vespertino':'Vespertino',
					u'Matutino': 'Matutino',
					u'Noturno': 'Noturno',
					u'N\xe3o aplica': 'Não aplica',
					u'Integral': 'Integral',
				}, warning=False, fill=True)

				yield EntriesParser.row_to_excel(fetched_data.values())

			print "last fetched: %s" % fetched_data
			print "#"*50


	def table7(self):
		"""
		gerando 7ª tabela.
		CST presenciais, em Instituições de Ensino Superior (IES) públicas, segundo o número de vagas por turno.
		"""

		mquery = lambda ano: QueryCreator(ano=ano,
			select=['qtde_de_cursos', 'vagas_turno', 'nat_juridica'],
			where=['tecnológico', 'ensino_público', 'data de início', 'educação presencial'], group_by=['vagas_turno','nat_juridica']).query

		# print "-"*40, "query example"
		# print mquery(2002)
		# print "-"*40, '\n'
	
		for year in ANOS_FHC+ANOS_LULA:
			cursor.execute(mquery(year))

			fetched_data = EntriesParser.parse_turno(cursor.fetchall())
			
			parsed_data = []
			for nat in fetched_data:
				parsed_data.append([fetched_data[nat], nat])

			# print parsed_data

			parsed_data = EntriesParser.filter_entries(parsed_data, {
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Municipal": "Municipal",
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Estadual": "Estadual",
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Federal": "Federal"
			}, fill=True)
		

			# print parsed_data
			# yield fetched_data
			# yield year, parsed_data

			yield EntriesParser.row_to_excel([year] + parsed_data.values())
			
		# print "last fetched: %s" % parsed_data
		print "#"*40


	def table8(self):
		"""
		gerando 8ª tabela.
		CST presenciais, na rede pública federal, segundo a organização acadêmica e o número de vagas por turno.
		"""

		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano: QueryCreator(ano=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'data de início', 'educação presencial'], group_by=['vagas_turno', "org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs), dado para ser adicionado à fetched_data
		mquery2 = lambda ano: QueryCreator(ano=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'data de início', 'universidade_federal', 'educação presencial'], group_by=['vagas_turno', "org_acad"]).query

		# mquery3 cria query específica para UTFs, dado para ser adicionado à fetched_data
		mquery3 = lambda ano: QueryCreator(ano=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'data de início', 'utf', 'educação presencial'], group_by=['vagas_turno', "org_acad"]).query

		cursor = self.cursor
	
		for year in ANOS_FHC+ANOS_LULA:
			data = dict()
			
			######################

			cursor.execute(mquery(year))
			fetched_data = EntriesParser.parse_turno(cursor.fetchall())

			# prepare data to filter the entries (from dict to tuple like (value, key))
			parsed_data = []
			for nat in fetched_data:
				parsed_data.append([fetched_data[nat], nat])
			
			data.update(EntriesParser.filter_entries(parsed_data, {
					"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
					"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
			}))

			######################

			cursor.execute(mquery2(year))
			fetched_data = EntriesParser.parse_turno(cursor.fetchall())

			# prepare data to filter the entries (from dict to tuple like (value, key))
			parsed_data = []
			for nat in fetched_data:
				parsed_data.append([fetched_data[nat], nat])
			
			data.update(EntriesParser.filter_entries(parsed_data, {
				'Universidade': 'UF',
			}))

			######################

			cursor.execute(mquery3(year))
			fetched_data = EntriesParser.parse_turno(cursor.fetchall())

			# prepare data to filter the entries (from dict to tuple like (value, key))
			parsed_data = []
			for nat in fetched_data:
				parsed_data.append([fetched_data[nat], nat])
			
			data.update(EntriesParser.filter_entries(parsed_data, {
				'Universidade': 'UTF'
			}))

			######################

			# yield data
			# yield year, data
			yield EntriesParser.row_to_excel([year] + data.values())
			
		print "last fetched: %s" % data
		print "#"*40


	def table9(self):
		"""
		gerando 9ª tabela.
		Licenciatura e bacharelado presencial, segundo a natureza jurídica (público e privado)
		"""

		mquery = lambda ano, cat_ensino: QueryCreator(
			ano=ano,
			select=['qtde_de_cursos', 'nat_juridica'],
			where=['licenciatura', 'educação presencial', cat_ensino],
		).query




# define globals
ANOS_FHC = range(1995, 2003)
ANOS_LULA = range(2003, 2011)

REGIOES = ('no sudeste', 'no nordeste', 'no sul', 'no norte', 'no centro-oeste')
MOD_ENSINO = ('educação presencial', 'educação a distância')
CATEGORIAS_ENSINO = ('ensino_público', 'ensino_privado')

if __name__ == "__main__":

	coon, cursor = connect_database()
	tg = TableGenerator(cursor)

	def show(datum):
		for data in datum:
		 	print data

	show(tg.table9())
