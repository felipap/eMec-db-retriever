#!/usr/bin/env python2.7
# -*- coding: UTF-8 -*-
"""
This module defines the TableGenerator class.

Part of the pydbcollector package.
by @f03lipe, 2011-2012
"""

from queryassembler import QueryAssembler
from dataparser import DataParser

import sys

# year ranges to iterate through
ANOS_FHC = range(1995, 2003)
ANOS_LULA = range(2003, 2011)

# SELECT and WHERE fields
REGIOES = ('no sudeste', 'no nordeste', 'no sul', 'no norte', 'no centro-oeste')
MOD_ENSINO = ('educação_presencial', 'educação_a_distância')
CATEGORIAS_ENSINO = ('ensino_público', 'ensino_privado')
TITULACAO = ('bacharelado', 'licenciatura')
NAT_JURIDICA_PUBLICA = ('municipal', 'estadual', 'federal')

parse_turno = DataParser.parse_turno
row_to_dict = DataParser.row_to_dict
filter = DataParser.filter


# auxiliary functions, to display cute messages
def _getWindowWidth():
	""" Return window width or fail (return None). """

	import os
	import idlelib
	import sys

	if _IN_IDLE:
		# running in idle, no way to know width
		return None

	def ioctl_GWINSZ(fd): # no idea what this is about
		try:
			import fcntl, termios, struct, os
			return struct.unpack('hh', fcntl.ioctl(fd,
				termios.TIOCGWINSZ, '1234'))
		except: pass

	cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
	if cr:
		return cr[1]
	try:
	   with os.open(os.ctermid(), os.O_RDONLY) as fd:
		   cr = ioctl_GWINSZ(fd)
		   return cr[0]
	except: pass

	try:
		return os.environ['COLUMNS']
	except: pass

	# last hope failed :(
	return None

def color(message, *config):
	""" Print a message colored, given the configuration wanted. """
	C = { 'HEADER': '\033[95m'
		, 'OKBLUE': '\033[94m'
		, 'OKGREEN': '\033[92m'
		, 'WARNING': '\033[93m'
		, 'FAIL': '\033[91m'
		, 'BOLD': '\033[1m'
		, 'NICEBLUE': '\033[36m'
		, "GLOW": "\033[32m" }
	
	if _IN_IDLE:
		# unix coloring doesn't work in idle
		return message
	
	ENDC = '\033[0m'
	sets = ' '.join(C[e] for e in config)
	return '%s%s%s' % (sets, message, ENDC)

def _centralize_msg(msg):
	""" Centralize message on the screen. """
	if _IN_IDLE or len(msg) > WIDTH:
		return msg
	spaces = ' ' * ((WIDTH-len(msg))/2)
	return spaces + msg + spaces
##

_IN_IDLE = bool('idlelib.run' in sys.modules)
WIDTH = _getWindowWidth() or 80

class TableGenerator(object):
	""" Generate data tables.

	Notes:
	the last_fetched message should allow the user to verify the configuration
	of the full data row, without the maniputation to the yield command. Most
	methods manipulate the display (or yield) of their rows, to make it easier
	for the user to copy-and-paste them to Excel.
	"""

	class _tg_metaclass(type):
		def __new__(self, name, parents, attributes):
			"""
			Decorate the table-generating static methods, to print their
			docstring on their call. Their doctring must be of the form:
			
				"gerando <table index>ª tabela. <table description>"

			Yes, this is the sole purpose of this metaclass!...
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
							# format and print the documentation, will ya?
							msg = 'DOC: '+' '.join([l.strip() for l in method.__doc__.split('\n') if (l.strip())])
							print WIDTH*'-'
							print color(_centralize_msg(msg), 'OKGREEN')
							print WIDTH*'-'
							return method(self, *args, **kwargs)
						return gen_method

					mod_attr[name] = decorator(name, value) # replace by decorator
			
			return type(name, parents, mod_attr)

	__metaclass__ = _tg_metaclass

	def __init__(self, cursor):
		self.cursor = cursor

	def table1(self):
		"""
		gerando 1ª tabela. CST presencial e à distância, segundo a natureza jurídica.
		"""
		cursor = self.cursor
		
		mquery = lambda ano, cat_ensino, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos'],
			where=['tecnológico', cat_ensino, mod_ensino, 'existia_no_ano']).query

		for mod_ensino in MOD_ENSINO:
			for cat_ensino in CATEGORIAS_ENSINO:
				print color("$ categoria de ensino: %s" % cat_ensino, 'OKBLUE')
				print color("$ modalidade de ensino: %s" % mod_ensino, 'OKBLUE')

				for year in ANOS_FHC+ANOS_LULA:
					cursor.execute(mquery(year, cat_ensino, mod_ensino))
					fetched_data = [e[0] for e in cursor.fetchall()]
					yield [year] + fetched_data
				
				print 

	def table2(self):
		"""
		gerando 2ª tabela. CST presencial e à distância, segundo a categoria administativa (municipal, estadual e federal).
		"""
		cursor = self.cursor
		
		mquery = lambda ano, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "nat_jurídica"],
			where=['tecnológico', 'ensino_público', 'existia_no_ano', mod_ensino],
			group_by=["nat_jurídica"], order_by=["nat_jurídica"]).query

		for mod_ensino in MOD_ENSINO:
			print color("$ modalidade de ensino: %s" % mod_ensino, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, mod_ensino))
				fetched_data = filter(cursor.fetchall(), {
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Municipal": "Municipal",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Estadual": "Estadual",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Federal": "Federal"
				}, autofill='0', default_size=3)
				yield [year] + [e[0] for e in fetched_data]
				
			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'
	
	def table3(self):
		"""
		gerando 3ª tabela. CST presencial e à distância, na rede pública federal, segundo a organização acadêmica.
		"""
		cursor = self.cursor

		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', mod_ensino],
			order_by=["org_acad"], group_by=["org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs)
		mquery2 = lambda ano, mod_ensino: QueryAssembler(year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', 'UF', mod_ensino],
			group_by=["org_acad"]).query
		
		# mquery3 cria query específica para UTFs
		mquery3 = lambda ano, mod_ensino: QueryAssembler(year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', 'UTF', mod_ensino],
			group_by=["org_acad"]).query

		for mod_ensino in MOD_ENSINO:
			print color("$ ensino: %s" % mod_ensino, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				fetched_data = []
				cursor.execute(mquery(year, mod_ensino))
				fetched_data += filter(cursor.fetchall(), {
					"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
					"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
				}, autofill='0')

				cursor.execute(mquery2(year, mod_ensino))
				fetched_data += filter(cursor.fetchall(), {
					'Universidade': 'UF',
				}, autofill='0')
				
				cursor.execute(mquery3(year, mod_ensino))
				fetched_data += filter(cursor.fetchall(), {
					'Universidade': 'UTF'
				}, autofill='0')
				yield [year] + fetched_data

			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table4(self):
		"""
		gerando 4ª tabela. CST presencial e à distância, na rede pública federal, segundo a organização acadêmica, por regiões.
		(3ª tabela dividida por regiões)
		"""
		cursor = self.cursor

		# generates table 4 (tabela 3 separada em regiões)
		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, regiao, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', regiao, mod_ensino],
			order_by=["org_acad"], group_by=["org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs)
		mquery2 = lambda ano, regiao, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', 'UF', regiao, mod_ensino],
			group_by=["org_acad"]).query
		
		# mquery3 cria query específica para UTFs
		mquery3 = lambda ano, regiao, mod_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['tecnológico', 'existia_no_ano', 'UTF', regiao, mod_ensino],
			group_by=["org_acad"]).query

		for mod_ensino in MOD_ENSINO:
			for regiao in REGIOES:
				print color("$ ensino: %s" % mod_ensino, 'OKBLUE')
				print color("$ regiao: %s" % regiao, 'OKBLUE')
				
				for year in ANOS_FHC+ANOS_LULA:
					fetched_data = []
					cursor.execute(mquery(year, regiao, mod_ensino))
					fetched_data += filter(cursor.fetchall(), {
						"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
						"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
					}, autofill='0')
					
					cursor.execute(mquery2(year, regiao, mod_ensino))
					fetched_data += filter(cursor.fetchall(), {
						'Universidade': 'UF',
					}, autofill='0')

					cursor.execute(mquery3(year, regiao, mod_ensino))
					fetched_data += filter(cursor.fetchall(), {
						'Universidade': 'UTF'
					}, autofill='0')
					yield [e[0] for e in fetched_data]
				
				print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), "\n"
	
	def table5_6(self):
		"""
		gerando 5ª e 6ª tabela. CST presenciais, em Instituições de Ensino Superior (IES) públicas e privadas, segundo o número de vagas por turno.
		"""
		cursor = self.cursor

		mquery = lambda ano, cat_ensino: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', 'vagas_turno'],
			where=['tecnológico', cat_ensino, 'existia_no_ano'],
			group_by=["vagas_turno"]).query

		for cat_ensino in CATEGORIAS_ENSINO:
			print color("$ categoria de ensino: %s" % cat_ensino, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, cat_ensino))                
				fetched_data = parse_turno(cursor.fetchall()).items()
				fetched_data = [(b,a) for a,b in fetched_data]

				fetched_data = filter(fetched_data, {
					u'Vespertino':'Vespertino',
					u'Matutino': 'Matutino',
					u'Noturno': 'Noturno',
					u'N\xe3o aplica': 'Nao se aplica',
					u'Integral': 'Integral',
				}, autofill='0')
				yield [e[0] for e in fetched_data]

			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), "\n"

	def table7(self):
		"""
		gerando 7ª tabela. CST presenciais, em Instituições de Ensino Superior (IES) públicas, segundo o número de vagas por turno.
		"""
		cursor = self.cursor
		
		mquery = lambda ano: QueryAssembler(year=ano,
			select=['qtde_de_cursos', 'vagas_turno', 'nat_jurídica'],
			where=['tecnológico', 'ensino_público', 'existia_no_ano', 'educação_presencial'],
			group_by=['vagas_turno','nat_jurídica']).query
	
		for year in ANOS_FHC+ANOS_LULA:
			cursor.execute(mquery(year))
			fetched_data = parse_turno(cursor.fetchall()).items()
			fetched_data = filter(fetched_data, {
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Municipal": "Municipal",
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Estadual": "Estadual",
				"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Federal": "Federal"
			}, keyindex=0, autofill='0')
			yield [e[1] for e in fetched_data]
			
		print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), "\n"

	def table8(self):
		"""
		gerando 8ª tabela. CST presenciais, na rede pública federal, segundo a organização acadêmica e o número de vagas por turno.
		"""
		cursor = self.cursor
		
		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano: QueryAssembler(year=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'existia_no_ano', 'educação_presencial'],
			group_by=['vagas_turno', "org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs)
		mquery2 = lambda ano: QueryAssembler(year=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'existia_no_ano', 'UF', 'educação_presencial'], 
			group_by=['vagas_turno', "org_acad"]).query

		# mquery3 cria query específica para UTFs
		mquery3 = lambda ano: QueryAssembler(year=ano,
			select=['qtde_de_cursos', 'vagas_turno', "org_acad"],
			where=['tecnológico', 'ensino_público', 'existia_no_ano', 'UTF', 'educação_presencial'],
			group_by=['vagas_turno', "org_acad"]).query

		for year in ANOS_FHC+ANOS_LULA:
			fetched_data = []

			cursor.execute(mquery(year))
			fetched_data += filter(cursor.fetchall(), {
				"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
				"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
			}, keyindex=2)

			cursor.execute(mquery2(year))
			fetched_data += filter(cursor.fetchall(), {
				'Universidade': 'UF',
			}, keyindex=2)

			cursor.execute(mquery3(year))
			fetched_data += filter(cursor.fetchall(), {
				'Universidade': 'UTF'
			}, keyindex=2)

			fetched_data = parse_turno(fetched_data)
			
			yield fetched_data.items()
		
		print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), "\n"

	def table9(self):
		"""
		gerando 9ª tabela. Licenciatura e bacharelado presencial, segundo a natureza jurídica (público e privado).
		"""
		cursor = self.cursor
		
		mquery = lambda ano, cat_ensino, tit: QueryAssembler(
			year=ano, select=['qtde_de_cursos'],
			where=['educação_presencial', 'existia_no_ano', cat_ensino, tit]).query

		for cat_ensino in CATEGORIAS_ENSINO:
			for tit in TITULACAO:
				print color("$ categoria de ensino: %s" % cat_ensino, 'OKBLUE')
				print color("$ titulação: %s" % tit, 'OKBLUE')

				for year in ANOS_FHC+ANOS_LULA:
					cursor.execute(mquery(year, cat_ensino, tit))
					fetched_data = cursor.fetchall()[0][0]
					yield year, fetched_data
				
				print

	def table10(self):
		"""
		gerando 10ª tabela. Licenciatura e bacharelado presencial, segundo a categoria administativa.
		"""
		cursor = self.cursor
		
		mquery = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', 'nat_jurídica'],
			where=['educação_presencial', 'existia_no_ano', tit],
			group_by=['nat_jurídica']).query

		for tit in TITULACAO:
			print color("$ titulação: %s" % tit, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, tit))
				fetched_data = filter(cursor.fetchall(), {
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Municipal": "Municipal",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Estadual": "Estadual",
					"Pessoa Jur\xc3\xaddica de Direito P\xc3\xbablico - Federal": "Federal"
				})
				yield [year]+[e[0] for e in fetched_data]
			
			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table11(self):
		"""
		gerando 11ª tabela. Licenciatura e Bacharelado presencial, na rede pública federal, segundo a organização acadêmica,
		"""
		cursor = self.cursor
		
		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['existia_no_ano', 'educação_presencial', tit],
			order_by=["org_acad"], group_by=["org_acad"]).query

		# mquery2 cria query específica para Universidades Federais (UFs)
		mquery2 = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['existia_no_ano', 'UF', 'educação_presencial', tit],
			group_by=["org_acad"]).query
		
		# mquery3 cria query específica para UTFs
		mquery3 = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad"],
			where=['existia_no_ano', 'UTF', 'educação_presencial', tit],
			group_by=["org_acad"]).query

		for tit in TITULACAO:
			print color("$ titulação: %s" % tit, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				fetched_data = []
				cursor.execute(mquery(year, tit))
				fetched_data += filter(cursor.fetchall(), {
					"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
					"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
				}, autofill='0')
				cursor.execute(mquery2(year, tit))
				fetched_data += filter(cursor.fetchall(), {
					'Universidade': 'UF'
				}, autofill='0')

				cursor.execute(mquery3(year, tit))
				fetched_data += filter(cursor.fetchall(), {
					'Universidade': 'UTF'
				}, autofill='0')
				yield [year] + [e[0] for e in fetched_data]
				
			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table12(self):
		"""
		gerando 12ª tabela. Licenciatura e bacharelado presenciais, em Instituições de Ensino Superior (IES) públicas, segundo o número de vagas por turno.
		"""
		cursor = self.cursor
		
		mquery = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', 'vagas_turno'],
			where=['ensino_público', 'existia_no_ano', tit],
			group_by=["vagas_turno"]).query

		for tit in TITULACAO:
			print color("$ titulação: %s" % tit, 'OKBLUE')
			
			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, tit))           
				fetched_data = parse_turno(cursor.fetchall()).items()
				fetched_data = [(b,a) for a,b in fetched_data]
				fetched_data = filter(fetched_data, {
					u'Vespertino':'Vespertino',
					u'Matutino': 'Matutino',
					u'Noturno': 'Noturno',
					u'N\xe3o aplica': 'Não aplica',
					u'Integral': 'Integral',
				}, autofill='0')
				yield [year] + fetched_data

			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table13(self):
		"""
		gerando 13ª tabela. Licenciatura e bacharelado presenciais, em Instituições de Ensino Superior (IES) privadas, segundo o número de vagas por turno.
		"""
		cursor = self.cursor
		
		mquery = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', 'vagas_turno'],
			where=['ensino_privado', 'existia_no_ano', tit],
			group_by=["vagas_turno"]).query

		for tit in TITULACAO:
			print color("$ titulação: %s" % tit, 'OKBLUE')

			for year in ANOS_FHC+ANOS_LULA:
				cursor.execute(mquery(year, tit))           
				fetched_data = parse_turno(cursor.fetchall()).items()
				fetched_data = [(b,a) for a,b in fetched_data]
				fetched_data = filter(fetched_data, {
					u'Vespertino':'Vespertino',
					u'Matutino': 'Matutino',
					u'Noturno': 'Noturno',
					u'N\xe3o aplica': 'Não aplica',
					u'Integral': 'Integral',
				}, autofill='0')
				yield [year] + fetched_data

			print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table14(self):
		"""
		gerando 14ª tabela. Licenciatura e bacharelado presenciais, segundo a natureza jurídica público (federal, estadual e municipal) e o número de vagas por turno.
		"""
		cursor = self.cursor
		
		mquery = lambda ano, nat_jur, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', 'vagas_turno'],
			where=['existia_no_ano', nat_jur, tit],
			group_by=['vagas_turno']).query

		for tit in TITULACAO:

			for nat_jur_pub in NAT_JURIDICA_PUBLICA:
				print color("$ titulação: %s" % tit, 'OKBLUE')
				print color("$ natureza jurídica pública: %s" % nat_jur_pub, 'OKBLUE')

				for year in ANOS_FHC+ANOS_LULA:
					cursor.execute(mquery(year, nat_jur_pub, tit))
					fetched_data = parse_turno(cursor.fetchall()).items()
					fetched_data = [(b,a) for a,b in fetched_data]

					fetched_data = filter(fetched_data, {
						u'Vespertino':'Vespertino',
						u'Matutino': 'Matutino',
						u'Noturno': 'Noturno',
						u'N\xe3o aplica': 'Nao se aplica?',
						u'Integral': 'Integral',
					}, autofill='0')
					yield [year] + fetched_data

				print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'

	def table15(self):
		"""
		gerando 15ª tabela. Licenciatura e bacharelado presenciais, segundo a natureza jurídica público (federal, estadual e municipal) e o número de vagas por turno.
		"""
		cursor = self.cursor

		# mquery pega os dados para IFETs e CEFETs
		mquery = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad", 'vagas_turno'],
			where=['existia_no_ano', 'educação_presencial', tit],
			order_by=["org_acad"], group_by=["org_acad", 'vagas_turno']).query

		# mquery2 cria query específica para Universidades Federais (UFs)
		mquery2 = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad", 'vagas_turno'],
			where=['existia_no_ano', 'UF', 'educação_presencial', tit],
			group_by=["org_acad", 'vagas_turno']).query
		
		# mquery3 cria query específica para UTFs
		mquery3 = lambda ano, tit: QueryAssembler(
			year=ano,
			select=['qtde_de_cursos', "org_acad", 'vagas_turno'],
			where=['existia_no_ano', 'UTF', 'educação_presencial', tit],
			group_by=["org_acad", 'vagas_turno']).query

		for tit in TITULACAO:

			for nat_jur_pub in NAT_JURIDICA_PUBLICA:
				print color("$ titulação: %s" % tit, 'OKBLUE')
				print color("$ natureza jurídica pública: %s" % nat_jur_pub, 'OKBLUE')

				for year in ANOS_FHC+ANOS_LULA:
					fetched_data = []
					cursor.execute(mquery(year, tit))
					fetched_data += filter(cursor.fetchall(), {
						"Centro Federal de Educa\xc3\xa7\xc3\xa3o Tecnol\xc3\xb3gica": "CEFET",
						"Instituto Federal de Educa\xc3\xa7\xc3\xa3o, Ci\xc3\xaancia e Tecnologia": "IFET",
					}, autofill='0')

					cursor.execute(mquery2(year, tit))
					fetched_data += filter(cursor.fetchall(), { 'Universidade': 'UF' }, autofill='0')
					cursor.execute(mquery3(year, tit))
					fetched_data += filter(cursor.fetchall(), { 'Universidade': 'UTF' }, autofill='0')

					# reorganize tuples to parse_turno() style
					fetched_data = [(a, str(c), str(b)) for (a, b, c) in fetched_data]
					fetched_data = parse_turno(fetched_data)

					yield [year] + fetched_data.items()
					
				print color("last fetched: %s" %  fetched_data, 'NICEBLUE'), '\n'
