#    sum_pr_map.py - NLPR mapper sum rank scores (used for normalisation)
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

total_pagerank_split = 0.0
total_prev_pagerank_split = 0.0

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	values = line.split()
	data_part = values[1]
	data_part = data_part.split("|")
	# Read rank score
	pagerank = data_part.pop()
	pagerank = float(pagerank)
	prev_data_part = float(values[2])
	total_pagerank_split += pagerank
	total_prev_pagerank_split += prev_data_part
# Output partial sum normalized and non normalized
print str(float(total_pagerank_split)) +"\t" + str(float(total_prev_pagerank_split))

# ------------------------------- #
