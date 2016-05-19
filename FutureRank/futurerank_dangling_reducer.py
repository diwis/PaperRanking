#    futurerank_dangling_reducer.py - FR's pagerank component reducer for dangling node sum
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

# Total dangling node score
total_dangling_score = 0

# ------------------------- #

# ---- Main Program / Reducer ---- #

# This should run in a single machine as it accumulates all partial sums

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Get float from line
	line = float(line)
	# Add score
	total_dangling_score += line

# output calculated score
print total_dangling_score

# -------------------------------- #
