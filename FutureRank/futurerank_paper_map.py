#    futurerank_paper_map.py - FR's paper score map step
#    Copyright (C) 2016  IMIS, Athena RC, Ilias Kanellos
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


# Mapper for futurerank PAPER scoring based on pagerank
# This will calculate all the coefficient parts (paper, author, time)
# It should output each coefficient separately

# ---- Imports ---- #

import sys
import math

# ----------------- #

# ---- Initialisations ---- #

exponential_ro = float(sys.argv[1])
query_year = sys.argv[2]

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Strip whitespace
	line = line.strip()
	# Here we do calculations based on paper - paper associations
	# IMPORTANT NOTE: paper IDS should start with digits - this should NOT be the case for authors
	if(line[0].isdigit()):
		# Break line to parts
		line_parts = line.split()
		# Get each required value
		pub_id = line_parts[0]
		pub_citation_data = line_parts[1]
		previous_pagerank = line_parts[2]
		pub_year = line_parts[3]
		# Get reference list
		pub_citation_data = pub_citation_data.split("|")
		reference_list = pub_citation_data[0]
		number_of_references = pub_citation_data[1]
		# Get current rank score
		current_rank = pub_citation_data[2]	
		
		# Output data for self first (otherwise, if no other ranks are passed to this key, we will lose it and reducer will crash)
		# Output values with corresponding coefficient
		print pub_id + "\t" + "alpha|0"
		# Output pmid previous data
		print pub_id + "\t" + "<" + reference_list + "|" + number_of_references + "|" + current_rank + "|" + pub_year + ">"
		
		# If there are papers referenced we output the score transferred to them. 
		# This should be done in a random walk probability fashion (Sayyadi's per email)
		if(number_of_references != "0"):
			reference_list = reference_list.split(",")
			# Loop references and output alpha value!
			for ref in reference_list:
				print ref + "\t" + "alpha|" + str(float(current_rank)/float(number_of_references))
		
		# Output also the exponential personalised score, based on publication year (coefficient gamma)
		# Sayyadi stated (mail) that current year should equal (max(Ti) + 1)
		exponent = exponential_ro * (int(query_year) + 1 - int(pub_year))
		personalised_time_factor = math.exp(exponent)
		print pub_id + "\t" + "gamma|" + str(personalised_time_factor)
		
	# ---------------------------------------------------------------- #
	# Here we output scores transferred by authors
	# Note that we will do author score transfer divided by outdegree 
	# (which will correspond to number of authors). This, again,
	# is to conform with Sayyadi's answers (per email).
	else:
		# Split author data - use tabs, because there are whitespaces in author names
		line_parts = line.split("\t")
		# Author name is the first field
		author = line_parts[0]
		# Previous score is always at the end
		previous_author_rank = line_parts[2]
		# Data contains: <pub1/author_num1,....,pubn/author_numn|current_author_score>
		author_paper_data = line_parts[1]
		author_paper_data = author_paper_data.split("|")
		author_pub_list = author_paper_data[0]
		current_author_rank = author_paper_data[1]
		# Loop author pubs and output author rank to them
		author_pub_list = author_pub_list.split(",")
		for author_pub in author_pub_list:
			# Divide author rank by number of authors for publication
			pub_id, num_authors = author_pub.rsplit("/", 1)
			print pub_id + "\t" + "beta|" + str(float(current_author_rank)/float(len(author_pub_list)))

# ------------------------------- #
	
