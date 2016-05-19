#    sum_pr_reduce.py - Reducer to find the sum of all pagerank scores
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

# This is our accumulator
pagerank_sum = 0

# ------------------------- #


# ---- Main Program / Reducer ---- #

# Read input lines and add partial sums calculated in the mappers.
# Therefore it should run on a single reducer
for line in sys.stdin:
	line = line.strip()
	pagerank_split = float(line)
	pagerank_sum += pagerank_split
	
# Output total sum
print pagerank_sum

# -------------------------------- #
