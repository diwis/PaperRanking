#    sum_coefficients_reduce.py - Reducer to normalize all partial score vectors
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


# Reducer for coefficient total calculation

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- # 

alpha_sum = 0.0
beta_sum = 0.0
gamma_sum = 0.0

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Since this is an accumulator, it should run on a single machine

# Read input line by line
for line in sys.stdin:
	line = line.strip()
	key, val = line.split()
	if key == "alpha":
		alpha_sum += float(val)
	elif key == "beta":
		beta_sum += float(val)
	elif key == "gamma":
		gamma_sum += float(val)

# Output totals
print str(alpha_sum) + "\t" + str(beta_sum) + "\t" + str(gamma_sum)

# -------------------------------- #
		
		
