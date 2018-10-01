# 	 weighted_citation.py - Implementation of Weighted Citation: an indicator of an article's prestige
#    Copyright (C) 2018  IMIS, Athena RC, Ilias Kanellos
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#    Contact email: ilias.kanellos@imis.athena-innovation.gr
#    Contact Address: Institute for the Management of Information Systems, Research Center "Athena", Artemidos 6 & Epidavrou, Marousi 15125, Greece

# -------------------------------------------------------------------- #

import os
import sys
import math

# -------------------------------------------------------------------- #

# Function Definitions
def get_journal_dict(journal_file):
	journal_dict = dict()
	with open(journal_file) as f:
		for line in f:
			line = line.strip()
			paper,journal = line.split("\t")
			journal_dict[paper] = journal.lower()
	return journal_dict
	
def get_impact_factor_dict(impact_factor_file):
	impact_factor_dict = dict()
	with open(impact_factor_file) as f:
		for line in f:
			line = line.strip()
			journal, score = line.split("\t")
			# print >> sys.stderr, "journal before:", journal
			impact_factor_dict[journal.lower()] = float(score)
	return impact_factor_dict
	
def get_year_dict(input_file):
	year_dict = dict()
	with open(input_file) as f:
		for line in f:
			line_parts = line.split()
			paper = line_parts[0]
			year  = line_parts[-1]
			year_dict[paper] = year
			
	return year_dict
	
########################################################################

def get_article_influence_dict(input_file, journal_dict, impact_factor_dict, current_year):
	# Store article influence scores here
	journal_article_influence_dict = dict()
	
	# Total papers in the period
	total_papers_in_period = 0
	# Count valid papers for each journal
	total_papers_in_journals_dict = dict()
	
	# Loop input file and count valid papers
	with open(input_file) as f:
		for line in f:
			line 		= line.strip()
			line_parts 	= line.split()
			year  		= line_parts[-1]
			paper 		= line_parts[0]
			journal		= journal_dict[paper]
			
			if journal not in total_papers_in_journals_dict:
				total_papers_in_journals_dict[journal] = 0
			
			# Check if paper is valid
			if (int(current_year) - int(year)) < 5:
				total_papers_in_period += 1
				total_papers_in_journals_dict[journal] += 1
	
	# Some journals may have NO papers published in the period examined. 
	# In this case we would have a division by zero
	for journal in total_papers_in_journals_dict:
		# print >>sys.stderr, "Journal:", journal
		if total_papers_in_journals_dict[journal] != 0:
			fraction_of_papers = float(total_papers_in_journals_dict[journal])/float(total_papers_in_period)
			journal_article_influence_dict[journal] = 0.01 * (float(impact_factor_dict[journal]) / float(fraction_of_papers))
		else:
			fraction_of_papers = float(1)/float(total_papers_in_period)
			journal_article_influence_dict[journal] = 0.01 * (float(impact_factor_dict[journal]) / float(fraction_of_papers))

		
	return journal_article_influence_dict
	
########################################################################
	

# -------------------------------------------------------------------- #

# -------------------------------------------------------------------- #

if (len(sys.argv) < 6):
	print "Usage: ./weighted_citation.py <graph_input_file> <total_journal_input_file> <impact_factor_file> <current_year> <exponential_coefficient>"
	sys.exit(0)

# Input
graph_input_file 	= sys.argv[1]
journal_input_file 	= sys.argv[2]
impact_factor_file	= sys.argv[3]
current_year		= sys.argv[4]
exponential_coef	= sys.argv[5]

exponential_coef = float(exponential_coef)

journal_dict = get_journal_dict(journal_input_file)
impact_factor_dict = get_impact_factor_dict(impact_factor_file)
journal_article_influence_dict = get_article_influence_dict(graph_input_file, journal_dict, impact_factor_dict, current_year)
year_dict = get_year_dict(graph_input_file)

'''
counter = 0
for j in sorted(journal_article_influence_dict, key=journal_article_influence_dict.get, reverse=True):
	print j + "\t" + str(journal_article_influence_dict[j])
	counter += 1
	if counter > 30:
		break
'''

weighted_citation_scores = dict()

# Loop input file and add scores to each paper
with open(graph_input_file) as f:
	for line in f: 
		line_parts = line.strip().split()
		
		paper = line_parts[0]
		year  = line_parts[-1]
		cited_list, cited_num, dummy_score = line_parts[1].split("|")
		cited_list = cited_list.split(",")
		
		if paper not in weighted_citation_scores:
			weighted_citation_scores[paper] = 0
		
		current_journal = journal_dict[paper]
		current_article_influence_score = journal_article_influence_dict[current_journal]
		
		if cited_list != "0" and cited_list != ["0"]:
			for cited_paper in cited_list:
				if cited_paper == '':
					print "Empty cited paper @ ", cited_list, "for", paper
				if cited_paper not in weighted_citation_scores:
					weighted_citation_scores[cited_paper] = 0
				
				cited_paper_year = year_dict[cited_paper]
				# Add transferred scores
				weighted_citation_scores[cited_paper] += current_article_influence_score * math.exp(exponential_coef * ( int(year) - int(cited_paper_year) ))

# Prin final scores
output_file = open("weighted_citation_" + graph_input_file + "", "w+")
for paper in sorted(weighted_citation_scores, key=weighted_citation_scores.get, reverse=True):
	output_file.write(paper + "\t" + str(weighted_citation_scores[paper]) + "\n")

output_file.close()
