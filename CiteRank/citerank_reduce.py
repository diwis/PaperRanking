#    citerank_reduce.py - This script is the reducer for a citerank iteration.
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

key = "initial_key"
prev_key = "initial_prev_key"
citerank_part = 0
# Create a dictionary containing publication data
current_data_dict = dict()
# Get alpha value
alpha = float(sys.argv[1])

# -------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split to key - val
	key, val = line.split()
	# If we have node data, store it and continue
	if(val.startswith("<")):
		current_data_dict[key] = val
		continue
	# If we don't have node data, simply get rank transferred
	val = float(val)
	if(key == prev_key):
		citerank_part  += val
	# Run this if the key changes: output data, reset variables
	else:
		if(prev_key != "initial_prev_key"):
			# Get pub data for previous key
			current_data = current_data_dict[prev_key]
			current_data = current_data.replace("<", "")
			current_data = current_data.replace(">", "")
			current_data = current_data.split("|")
			outlinks = current_data[0]
			outlink_num = current_data[1]
			running_citerank = current_data[-3]
			prev_citerank = running_citerank
			publication_year = current_data[-1]
			# Partially calculate CR of iteration. 
			current_citerank_part = alpha * citerank_part
			citerank = float(running_citerank) + float(current_citerank_part)
			# Write output for key that changed. CR score is on CR. Current_citerank_part is the addend calculated in this step. Prev_citerand was the accumulated sum up to the previous step
			print prev_key + "\t" + outlinks + "|" + outlink_num + "|" + str(current_citerank_part) + "\t" + str(citerank) + "\t" + str(prev_citerank) + "\t" + publication_year		
			current_data_dict.pop(prev_key, None)
			# Reset variables
			citerank_part = 0
			citerank = 0
			prev_key = key
			citerank_part += val
		# This should run if we are at the first key
		else: 
			prev_key = key
			citerank_part  += val

# Last key may not have been output. Do this now
if(key == prev_key):
	# Repeat the above
	current_data = current_data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	current_data = current_data.split("|")
	current_data_dict.pop(key, None)
	outlinks = current_data[0]
	outlink_num = current_data[1]
	running_citerank = current_data[-3]
	prev_citerank = running_citerank
	publication_year = current_data[-1]
	current_citerank_part = alpha * citerank_part
	citerank = float(running_citerank) + float(current_citerank_part)
	print key + "\t" + outlinks + "|" + outlink_num + "|" + str(current_citerank_part) + "\t" + str(citerank) + "\t" + str(prev_citerank) + "\t" + publication_year

# -------------------------------- #
