#    futurerank_paper_reduce.py - FR's paper score reduce step
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


# Reducer for futurerank PAPER score calculations

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

# Read command line arguments
dangling_sum = float(sys.argv[1])
num_nodes = float(sys.argv[2])
num_authors = float(sys.argv[3])

key = "initial_key"
prev_key = "initial_prev_key"
current_data = ""
alpha_sum = 0
beta_sum = 0
gamma_sum = 0

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Get key and value
	key, val = line.split()
	
	# ---------------------------------------------------------------- #
	
	# If we just get paper data, continue, but keep the data in the dictionary
	if(val.startswith("<")):
		print line
		continue
	
	# ---------------------------------------------------------------- #
	
	# If we are on the same key as previously, simply add the value where needed
	if(key == prev_key):
		coefficient, value = val.split("|")
		if(coefficient == "alpha"):
			alpha_sum += float(value)
		elif(coefficient == "beta"):
			beta_sum += float(value)
		elif(coefficient == "gamma"):
			gamma_sum += float(value)
	
	# ---------------------------------------------------------------- #
	
	# Else if key changed and we are not just starting, re-initialise variables
	elif(prev_key != "initial_prev_key"):
		alpha_sum += dangling_sum
		print prev_key + "\tpr\t" + str(alpha_sum)
		print prev_key + "\tat\t" + str(beta_sum)
		print prev_key + "\tti\t" + str(gamma_sum)
		# Re-initialisations
		prev_key = key
		alpha_sum = 0
		beta_sum = 0
		gamma_sum = 0
		coefficient, value = val.split("|")
		if(coefficient == "alpha"):
			alpha_sum += float(value)
		elif(coefficient == "beta"):
			beta_sum += float(value)
		elif(coefficient == "gamma"):
			gamma_sum += float(value)

	# ---------------------------------------------------------------- #
	
	# Run this if Previous key == initial_prev_key
	else:
		prev_key = key
		alpha_sum = 0
		beta_sum = 0
		gamma_sum = 0
		coefficient, value = val.split("|")
		if(coefficient == "alpha"):
			alpha_sum += float(value)
		elif(coefficient == "beta"):
			beta_sum += float(value)
		elif(coefficient == "gamma"):
			gamma_sum += float(value)
			
# -------------------------------------------------------------------- #

# Final entry may not have been output. Do this now
if(key == prev_key):
	alpha_sum += dangling_sum
	print key + "\tpr\t" + str(alpha_sum)
	print key + "\tat\t" + str(beta_sum)
	print key + "\tti\t" + str(gamma_sum)

# ------------------------- #
