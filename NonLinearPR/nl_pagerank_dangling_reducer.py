#    nl_pagerank_dangling_reducer.py - NLPR reducer for dangling node score transfer calculation
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

dangling_pagerank = 0

# ------------------------- #

# ---- Main program / Mapper ---- #

# This should run on a single machine, as it 
# accumulated results calculated in the mappers

# Read line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split to key - val
	key, val = line.split()
	val = float(val)
	# Accumulate
	dangling_pagerank += val
# Output accumulated score	
print float(dangling_pagerank)

# ------------------------------- #
