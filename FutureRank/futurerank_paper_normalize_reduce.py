#    futurerank_paper_normalize_reduce.py - Reducer to normalize all partial score vectors
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


# Reducer to calculate normalized scores based on futurerank

# ---- Imports ---- #

import sys

# ----------------- #

# ---- Initialisations ---- #

# Get command line arguments
alpha_total = float(sys.argv[1])
beta_total = float(sys.argv[2])
gamma_total = float(sys.argv[3])

key = "none"
prev_key = "none_prev"
alpha_val = 0.0
beta_val = 0.0
gamma_val = 0.0

alpha = float(sys.argv[4])
beta = float(sys.argv[5])
gamma = float(sys.argv[6])
num_nodes = sys.argv[7]

running_alpha_total = 0.0
running_beta_total = 0.0
running_gamma_total = 0.0

data_dict = dict()

# ------------------------- #

# ---- Main Program / Reducer ---- #

# This reducer can run in parallel, since it normalizes scores

# Read input line by line
for line in sys.stdin:
	line = line.strip()
	line = line.split()
	# If we have the publication's previous data, 
	# just store it temporarily
	if(len(line) == 2):
		key = line[0]
		val = line[1]
		data_dict[key] = val
		continue
	# If we read calculation data, then
	else:	
		key = line[0]
		# if Key == first key
		if(prev_key == "none_prev"):
			prev_key = key
			val = line[-1]
			coefficient = line[-2]
			# Recalculate each coefficient
			if(coefficient == "pr"):
				alpha_val = float(val)/float(alpha_total)
				running_alpha_total += alpha_val
			elif(coefficient == "at"):
				beta_val = float(val)/float(beta_total)
				running_beta_total += beta_val
			elif(coefficient == "ti"):
				gamma_val = float(val)/float(gamma_total)
				running_gamma_total += gamma_val
		# If key == previous key
		elif(key == prev_key):
			val = line[-1]
			coefficient = line[-2]
			if(coefficient == "pr"):
				alpha_val = float(val)/float(alpha_total)
				running_alpha_total += alpha_val
			elif(coefficient == "at"):
				beta_val = float(val)/float(beta_total)
				running_beta_total += beta_val
			elif(coefficient == "ti"):
				gamma_val = float(val)/float(gamma_total)
				running_gamma_total += gamma_val
		# IF key changes
		else:
			current_data = data_dict[prev_key]
			current_data = current_data.replace("<", "")
			current_data = current_data.replace(">", "")
			current_data = current_data.split("|")
			outlinks = current_data[0]
			outlink_num = current_data[1]
			old_futurerank = current_data[2]
			publication_year = current_data[3]	
			# Calculate futurerank's vector coordinate values	
			futurerank = alpha * alpha_val + beta * beta_val + gamma * gamma_val + (1 - alpha - beta - gamma) * 1/float(num_nodes)
			print prev_key + "\t" + outlinks + "|" + outlink_num + "|" + str(futurerank) + "\t" + old_futurerank + "\t" + publication_year
			# Reset calculation variables
			data_dict.pop(prev_key, None)
			prev_key = key
			alpha_val = 0.0
			beta_val = 0.0
			gamma_val = 0.0
			val = line[-1]
			coefficient = line[-2]
			if(coefficient == "pr"):
				alpha_val = float(val)/float(alpha_total)
				running_alpha_total += alpha_val
			elif(coefficient == "at"):
				beta_val = float(val)/float(beta_total)
				running_beta_total += beta_val
			elif(coefficient == "ti"):
				gamma_val = float(val)/float(gamma_total)	
				running_gamma_total += gamma_val		

# Last key may not have output any data, we do this now
if(key == prev_key):
	if(coefficient == "pr"):
		alpha_val = float(val)/float(alpha_total)
		running_alpha_total += alpha_val
	elif(coefficient == "at"):
		beta_val = float(val)/float(beta_total)
		running_beta_total += beta_val
	elif(coefficient == "ti"):
		gamma_val = float(val)/float(gamma_total)
		running_gamma_total += gamma_val
	current_data = data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	current_data = current_data.split("|")
	outlinks = current_data[0]
	outlink_num = current_data[1]
	old_futurerank = current_data[2]
	publication_year = current_data[3]		
	futurerank = alpha * alpha_val + beta * beta_val + gamma * gamma_val + (1 - alpha - beta- gamma) * 1/float(num_nodes)
	print key + "\t" + outlinks + "|" + outlink_num + "|" + str(futurerank) + "\t" + old_futurerank + "\t" + publication_year

# -------------------------------- #
	
