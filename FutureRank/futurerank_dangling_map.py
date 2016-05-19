#    futurerank_dangling_map.py - FR's pagerank component mapper for dangling node sum
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

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

# Sum of dangling node ranks
dangling_sum = 0
# Num of papers
node_count = int(sys.argv[1])

# -------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Strip whitespace
	line = line.strip()
	# Get line parts
	line_parts = line.split()
	# First string is article id
	pub_id = line_parts[0]
	# Data <cited|num_citations|current_rank> is the second field
	pub_data = line_parts[1]
	pub_data = pub_data.split("|")
	reference_list = pub_data[0]
	num_of_references = pub_data[1]
	current_score = pub_data[2]
	# Previous score is next field
	previous_score = line_parts[2]
	# Final Field is year
	pub_year = line_parts[3]
	
	# According to Hassan Sayyadi (per mail), futurerank should have random walk probabilities in its matrices
	if(num_of_references == "0"):
		# Add the probability of jumping to any node
		dangling_sum += float(current_score)/node_count
	else:
		# If we have outgoing links, we don't need to do anything
		continue

# Finally output summed dangling score
print dangling_sum

# ------------------------------- #
