#    yetrank_dangling_reducer.py - This is the script to calculate 
#			  	  - the sum of dangling node YetRank scores
#
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

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

# Dangling node 
# pagerank score accumulator
dangling_yetrank = 0

# ------------------------- #

# ---- Main program / Reducer ---- #

# This should function as an accumulator
# Thus should run on a single reducer
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split to key - val
	key, val = line.split()
	val = float(val)
	# Accumulate scores
	dangling_yetrank += val

# Output accumulated sum	
print dangling_yetrank

# -------------------------------- #
