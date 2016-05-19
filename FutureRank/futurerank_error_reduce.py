#    futurerank_error_reduce.py - Reducer for max error calculation
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


# Reducer for error calculation

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

total_error = 0.0

# ------------------------- #

# ---- Main program / Reducer ---- #

# This should run on a single machine

# Read input line by line as it accumulates partial results
for line in sys.stdin:
	line = line.strip()
	error_part = float(line)
	total_error += error_part
# Output total
print total_error

# -------------------------------- #

	
