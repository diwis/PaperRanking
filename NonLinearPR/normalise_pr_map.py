#    normalise_pr_map.py - NLPR mapper to normalise scores - no reduce needed here
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

normalise_value = float(sys.argv[1])
prev_normalise_value = float(sys.argv[2])

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Get pagerank value
	data_part = line.split()
	pmid = data_part[0]
	prev_pagerank = float(data_part[2])
	publication_year = data_part[3]
	data_part = data_part[1]
	data = data_part.split("|")
	pagerank = float(data.pop())
	normalised_pagerank = pagerank * normalise_value
	normalised_previous_pagerank = prev_pagerank * prev_normalise_value
	print pmid + "\t" + data[0] + "|" + data[1] + "|" + str(float(normalised_pagerank)) + "\t" + str(float(normalised_previous_pagerank)) + "\t" + publication_year

# ------------------------------- #

