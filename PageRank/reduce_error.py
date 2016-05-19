#    reduce_error.py - Reducer of error calculation
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

# ---- Initialisations ----- #

# Start with a zero value for
# max error. Replace everytime
# we get a higher value
max_error = 0

# -------------------------- #

# ---- Main Program / Reducer ---- #

# Read input - Remember on hadoop this should run on a single reducer
for line in sys.stdin:
	# Read line and replace max_error with higher values
	line = line.strip()
	if(float(line) >= max_error):
		max_error = float(line)

# Output the highest value	
print max_error

# -------------------------------- #
