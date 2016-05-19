#    map_error.py - This script is the mapper for calculating PageRank's error.
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
import math

# ----------------- #


# --- Initialisations ---- #

# We need to find the maximum error.
# Initialise its value to 0.
max_error = 0

# ------------------------ #


# ---- Main program / Mapper ---- #

# Read input from stdin (standard map-reduce procedure)
# The input should be formatted by the output of the 
# PageRank reducer used for a simple calculation of an iteration.
for line in sys.stdin:
	# Remove surrounding whitespace
	line = line.strip()
	# Split line into parts
	line = line.split()
	# The final part of the output is the year.
	# We remove it, since we don't need it.
	year = line.pop()
	# Get previous pagerank (what's left at the end after removing the year).
	prev_pagerank = float(line.pop())
	# Get current pagerank
	current_data = line.pop()
	# Current pagerank data contains list of referenced papers,
	# number of referenced papers and the current pagerank value
	current_data = current_data.split("|")
	# We only need the current PageRank value.
	current_pagerank = float(current_data.pop())
	# Calculate the absolute difference of pagerank values.
	difference = math.fabs(current_pagerank - prev_pagerank)
	# Replace max error, if the difference found is larger
	if(difference >= max_error):
		max_error = difference

# After reading all lines, output the maximum value
print max_error

# -------------------------------- #
