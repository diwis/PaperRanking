#    timeawarerank_error_reduce.py - TAR-RAM/TAR-ECM error calculation reduce script
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

total_max_error = 0.0

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line. This should be run on a single 
# reducer, as we need the maximum of all values output by mappers
for line in sys.stdin:
	error = float(line.strip())
	if(error > total_max_error):
		total_max_error = error

# Output Value of maximum error
print total_max_error
	
# -------------------------------- #
