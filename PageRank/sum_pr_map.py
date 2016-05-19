#    sum_pr_map.py - Mapper to find the sum of all pagerank scores
#
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

total_pagerank_split = 0

# ------------------------- #

# ---- Main program / Mapper ---- #

# Read input lines, sum result.
# This will be a partial sum of the total
# (the latter will be calculated on a single reducer)
for line in sys.stdin:
	line = line.strip()
	values = line.split()
	data_part = values[1]
	data_part = data_part.split("|")
	pagerank = data_part.pop()
	pagerank = float(pagerank)
	total_pagerank_split += pagerank

# Output the partial sum
print total_pagerank_split

# ------------------------------- #
