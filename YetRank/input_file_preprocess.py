#    input_file_preprocess.py - Preprocess classic citation graph input & paper-journal input to get
#    weights for YetRank.
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

# YetRank has 2 modes:
# i) Using only Impact Factor weights
# i) Using both Impact Factor and publication age-based weights
# We implement both options here.

# -------------------------------------------------------------------- #

import sys
import math

# -------------------------------------------------------------------- #

def get_journal_dict(paper_journal_file):
	journal_dict = dict()
	with open(paper_journal_file) as f:
		for line in f:
			# print >>sys.stderr, "Line:", line
			line = line.strip()
			if line == "":
				continue
			paper, journal = line.split('\t')
			# print "Paper is: ", paper
			journal_dict[paper] = journal.lower()
	return journal_dict

########################################################################

def get_pub_years(input_file):
	pub_year_dict = dict()
	with open(input_file) as f:
		for line in f:
			line = line.strip()
			if line == "":
				continue
			line_parts = line.split()
			pub = line_parts[0]
			year = line_parts[-1]
			pub_year_dict[pub] = year 
	return pub_year_dict
	
########################################################################

def normalize_dict(dictionary, norm_factor):
	new_dict = dict()
	for key in dictionary:
		new_dict[key] = float(dictionary[key])/float(norm_factor)
		
	return new_dict

########################################################################

def get_pub_age_weight(pub_year, current_year, exponent_tau):
	if int(float(exponent_tau)) == 0:
		return 1
	pub_age = int(current_year) - int(pub_year)
	exponent_factor = math.exp(-(float(pub_age)/float(exponent_tau)))
	
	final_rho = float(exponent_factor)/float(exponent_tau)
	return final_rho

########################################################################	

# -------------------------------------------------------------------- #

if (len(sys.argv) < 5):
	print "Usage:"
	print "./input_file_preprocess.py <citation_graph_input_file> <paper_to_journal_file> <current_year> <year_range_for_impact_factor> <optional:exponent>"
	sys.exit(0)

input_file 			= sys.argv[1]
paper_journal_file	= sys.argv[2]
current_year		= sys.argv[3]
impact_factor_range = sys.argv[4]
try:
	exponent		= sys.argv[5]
except:
	exponent		= 0

	
# Dictionary that will contain all impact factors
impact_factor_dict 	= dict()
pub_age_weights		= dict()

# Citable items in range
num_citable = 0

# -------------------------------------------------------------------- #

journal_dict 	= get_journal_dict(paper_journal_file)
pub_years		= get_pub_years(input_file)
start_year 		= int(current_year) - int(impact_factor_range)
end_year   		= int(current_year)-1

'''
print "Counting citations from", start_year, "to", end_year
print "Calculating IF for", current_year
'''

# Loop lines in input graph file

# First calculate all cited papers published in timespan, per journal
# while also counting citable items
# Then, divide by citeable items (common number for all journals)
with open(input_file) as f:
	for line in f:
		# print >>sys.stderr, "Line: ", line
		line = line.strip()
		if line == "":
			continue
		pub, pub_data, prev_score, year = line.split()
		
		pub_age_weights[pub] = get_pub_age_weight(year, current_year, exponent)
		
		pub_journal = journal_dict[pub]
		if pub_journal not in impact_factor_dict:
			# Add a minimal impact factor of 1/#citeables to avoid zero weights
			impact_factor_dict[pub_journal] = 1
		
		# Add as citeable item if published in particular range
		if int(year) <= end_year and int(year) >= start_year:
			# One more citeable item
			num_citable +=1 
			
		# If paper was published in current year, check citations
		# to papers published in the speficied year span
		if int(year) == int(current_year):
			# get publication list
			pub_list = pub_data.split("|")[0].split(",")
			if pub_list == ["0"]:
				continue
			
			# for each cited paper, if it was published in year range,
			# add a citation to the corresponding journal
			for cited_pub in pub_list:
				# Get journal of cited pub
				cited_pub_journal = journal_dict[cited_pub]
				if cited_pub_journal not in impact_factor_dict:
					# Add a minimal impact factor of 1/#citeables to avoid zero weights
					impact_factor_dict[cited_pub_journal] = 1
				# Add citation if cited pub is in range
				cited_pub_year = pub_years[cited_pub]
				if int(cited_pub_year) >= start_year and int(cited_pub_year) <= end_year:
					impact_factor_dict[cited_pub_journal] += 1
					
# We should now have a dictionary of all valid citations in the specified year range, per journal
# We have to divide each entry by #citeable items now
impact_citations_dict = impact_factor_dict
impact_factor_dict = normalize_dict(impact_factor_dict, num_citable)

# print "Impact factor sum: ", sum(impact_factor_dict.values())
impact_factor_sum = sum(impact_factor_dict.values())

'''
print "Citeable Items: ", num_citable

for journal in impact_factor_dict:
	print journal + "\t" + str(impact_factor_dict[journal])

for journal in impact_citations_dict:
	print journal + "\t" + str(impact_citations_dict[journal])
'''	

impact_factor_file = open("impact_factors_" + str(current_year) + "_" + str(impact_factor_range) + "_" + input_file + "", "w")
for journal in impact_factor_dict:
	impact_factor_file.write(journal + "\t" + str(impact_factor_dict[journal]) + "\n")
impact_factor_file.close()	


weight_sum = 0.0
final_weight_dict = dict()
for paper in pub_age_weights:
	# print "Pub age weight: ", pub_age_weights[paper]
	weight_sum += pub_age_weights[paper] * impact_factor_dict[journal_dict[paper]]
	
for paper in pub_age_weights:
	final_weight_dict[paper] = (pub_age_weights[paper] * impact_factor_dict[journal_dict[paper]]) / weight_sum
	
with open(input_file) as f:
	for line in f:
		line = line.strip()
		pub  = line.split()[0]
		print line + "\t" + str(final_weight_dict[pub])

	
