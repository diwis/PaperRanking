#    futurerank_sum_score_reduce.py - Calculate total scores of authors/papers reducer
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


# Add author/paper scores calculated by mappers
# This should run on a single machine, as is accumulates all scores

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

total_score = 0

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line, add scores
for line in sys.stdin:
	line = line.strip()
	score = float(line)
	total_score += score
# Output result
print total_score

# -------------------------------- #
