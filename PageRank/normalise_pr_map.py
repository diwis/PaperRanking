#    normalise_pr_map.py - This script is the mapper for normalizing final PageRank scores
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

# This script should be passed as input the 
# total sum of pagerank scores calculated for 
# all papers. It may not be exactly 1, so we normalize it.
nomalise_value = float(sys.argv[1])

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line (standard map-reduce procedure)
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split input line into parts
	data_part = line.split()
	# pmid here is the identifier of the publication
	pmid = data_part[0]
	# Get previous pagerank score
	prev_pagerank = data_part[2]
	# Get year of publication
	publication_year = data_part[3]
	# Get pagerank score, list and number of referenced papers
	data_part = data_part[1]
	data = data_part.split("|")
	pagerank = float(data.pop())
	# Calculate the normalized score
	normalised_pagerank = pagerank * nomalise_value
	# Output the original input line, having changed only the pagerank score
	print pmid + "\t" + data[0] + "|" + data[1] + "|" + str(normalised_pagerank) + "\t" + str(pagerank) + "\t" + publication_year

# ------------------------------- #
