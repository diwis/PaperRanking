#    futurerank_normalize_map.py - Output normalized scores for authors
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

# Read command line argument
score_sum = float(sys.argv[1])

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	line_parts = line.split("\t")
	line_data = line_parts[1]
	line_data = line_data.split("|")
	# remove non-normalized score
	abnormal_score = float(line_data.pop())
	normalized_score = abnormal_score/score_sum
	line_parts[1] = "|".join(line_data) + "|" + str(normalized_score)
	# Print line with normalized score
	outstring = "\t".join(line_parts)
	print outstring

# ------------------------------ #

