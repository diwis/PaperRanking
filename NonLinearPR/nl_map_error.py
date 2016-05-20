#    nl_map_error.py - NLPR error calcluation mapper
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

# ---- Initialisations ---- #

# Max value of diffs
max_error = 0

# ------------------------- #

# ---- Main Program / Mapper ---- # 

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split line into parts
	line = line.split()
	# Remove year
	year = line.pop()
	# Get previous pagerank
	prev_pagerank = float(line.pop())
	# Get current pagerank
	current_data = line.pop()
	current_data = current_data.split("|")
	current_pagerank = float(current_data.pop())
	# Calculate their difference
	difference = math.fabs(current_pagerank - prev_pagerank)
	if(difference > max_error):
		max_error = difference
# Output max error found		
print max_error

# ------------------------------ #
