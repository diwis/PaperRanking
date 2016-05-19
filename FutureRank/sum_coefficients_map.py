#    sum_coefficients_map.py - Mapper to normalize all partial score vectors
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


# Sum futurerank coefficient values in order to normalize

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

alpha_sum = 0.0
beta_sum = 0.0
gamma_sum = 0.0

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	line = line.strip()
	# Get line parts
	line = line.split()
	# There should be at least 3 parts, 
	# in order to have a coefficient's entry
	if(len(line) > 2):
		if line[1] == "pr":
			alpha_sum += float(line[-1])
		elif line[1] == "at":
			beta_sum += float(line[-1])
		elif line[1] == "ti":	 
			gamma_sum += float(line[-1])
			
# Output partial scores
print "alpha\t" + str(alpha_sum)
print "beta\t" + str(beta_sum)
print "gamma\t" + str(gamma_sum)

# ------------------------------- #
