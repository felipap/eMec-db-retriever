#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
This module defines the QueryAssembler class.

Part of the pydbcollector package.
by @f03lipe, 2011-2012
"""


class QueryAssembler(object):
	""" Assembles an SQL query to retrieve data from server.

	This class defines default query snippets to be added to the queries,
	as the __init__ arguments dictate. The two mapping objects are SELECT
	and WHERE, which contain snippets of sql queries for SELECT and WHERE
	clauses, respectively.

	!!!!
	This is not to be used as a full interface, as not every combination of
	SELECT and WHERE fields yields something meaningfull or even error-free. 

	"""


	SELECT = {
		'qtde_de_cursos': 	"count (*) as qtde_cursos",
		'nat_jurídica':		"n.natureza_juridica as nat_jurídica",
		'org_acad':			"o.organizacao_academica as org_acad",
		'estado':			"i.uf as estado",
		'modalidade':		"m.nome as modalidade",
		'vagas_turno':		"c.vagas_totais_anuais as vagas_turno"
	}

	WHERE = {
		'__base': 			"i.natid = n.id and i.instid = c.instid and c.modid = m.id and c.titid = t.id and i.orgid = o.id", # the basic condition that holds all tables together
		
		'tecnológico':		"t.nome = 'Tecnológico'",
		'licenciatura': 	"t.nome = 'Licenciatura'",
		'bacharelado':		"t.nome = 'Bacharelado'",

		'ensino_público':
			"""((n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Municipal') or (n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Estadual') or
				(n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Federal'))""",
		'ensino_privado':
			"""((n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Associação de Utilidade Pública') or (n.natureza_juridica = 'Privada sem fins lucrativos')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Sociedade') or (n.natureza_juridica = 'Privada com fins lucrativos')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Mercantil ou Comercial')
				or (n.natureza_juridica = 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Associação de Utilidade Pública'))""",
		
		'existia_no_ano':
				"""(SUBSTRING(c.data_inicio FROM 7 FOR 10) < '{year}') and not (SUBSTRING(c.data_inicio FROM 7 FOR 10) = '') and
				(SUBSTRING(c.data_inicio FROM 7 FOR 10) > '1900')""", # for incorrect entries

		'UF':	"(i.nome LIKE '%UNIVERSIDADE FEDERAL%')",
		"UTF":	"(SUBSTRING(i.nome FROM 1 FOR 32) = 'UNIVERSIDADE TECNOLÓGICA FEDERAL')",

		'no norte':			"(i.uf = 'AM' or i.uf = 'AC' or i.uf = 'PA' or i.uf = 'RO' or i.uf = 'RR' or i.uf = 'TO' or i.uf = 'AP')",
		'no sul':			"(i.uf = 'SC' or i.uf = 'PR' or i.uf = 'RS')",
		'no nordeste':		"(i.uf = 'AL' or i.uf = 'BA' or i.uf = 'CE' or i.uf = 'MA' or i.uf = 'PB' or i.uf = 'PE' or i.uf = 'PI' or i.uf = 'RN' or i.uf = 'SE')",
		'no centro-oeste':	"(i.uf = 'GO' or i.uf = 'MT' or i.uf = 'MS' or i.uf = 'DF')",
		'no sudeste':		"(i.uf = 'RJ' or i.uf = 'ES' or i.uf = 'SP' or i.uf = 'MG')",

		'educação_presencial':	"(m.nome = 'Educação Presencial')",
		'educação_a_distância':	"(m.nome = 'Educação a Distância')",

		'municipal': 		"(n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Municipal')",
		'estadual':			"(n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Estadual')",
		'federal':			"(n.natureza_juridica = 'Pessoa Jurídica de Direito Público - Federal')"
	}


	def __init__(self, year, select, where=None, group_by=None, order_by=None):
		
		# this here is a huge mess!!  \\O
		#                              /
		#                             /\

		self.query = ""
		self.year = year

		self.add_select(select)
		self.add_from()
		self.add_where(where)
		self.add_group_by(group_by)
		self.add_order_by(order_by)

	def add_to_query(self, *pieces):
		""" Add sql pieces to the query. """
		if pieces[0].isupper():
			self.query += "\n"
		self.query += " ".join(pieces)+" "

	def add_select(self, conditions):
		""" Add the SELECT field. """
		self.add_to_query("SELECT")
		assert 'qtde_de_cursos' in conditions, "select qtde_de_cursor"
		self.add_to_query(', '.join(self.SELECT[c] for c in conditions))

	def add_from(self):
		""" Add the default FROM field. """
		self.add_to_query("FROM curso c, instituicao i, titulacao t, modalidade m, organizacao o, natureza n")

	def add_where(self, conditions):
		""" Add WHERE field. """
		assert conditions, "no conditions given"
		conditions.append('__base') # add base condition

		d = dict()
		for c in conditions:
			d[c] = self.WHERE[c]
			if c == "existia_no_ano":
				d[c] = d[c].format(year=self.year+1)
		self.add_to_query("WHERE", ' and '.join(d.values()))

	def add_group_by(self, clauses):
		if clauses:
			self.add_to_query("group by")
			self.add_to_query(', '.join(clauses))

	def add_order_by(self, clauses):
		if clauses:
			self.add_to_query("order by")
			self.add_to_query(', '.join(clauses))