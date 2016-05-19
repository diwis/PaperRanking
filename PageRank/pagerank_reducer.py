#    pagerank_map.py - Reducer of a single PageRank iteration
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


# ---- Initialisations ---- #

key = "initial_key"
prev_key = "initial_prev_key"
pagerank = 0
current_data = ""
# Create a dictionary for publication data
current_data_dict = dict()
# Read alpha value
alpha = float(sys.argv[1])
# Get dangling node pagerank sum as an argument
dangling_pagerank = float(sys.argv[2])
# Count nodes
node_count = int(sys.argv[3])

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Split to key - val pair
	key, val = line.split()
	# If we have node data, store it and continue - this line is the previous data for a given paper
	if(val.startswith("<")):
		current_data_dict[key] = val
		continue
	# If we don't have node data, simply get the pagerank value transfered from a citing paper
	val = float(val)
	# Get key - if no change to the key, add pagerank to other scores received
	if(key == prev_key):
		pagerank += val
	# If key changes
	else:
		# If we are not at the first key of the reducer, we have to output data calculated for the previous key
		if(prev_key != "initial_prev_key"):
			# Get the previous data and remove "<", ">"
			current_data = current_data_dict[prev_key]		
			current_data = current_data.replace("<", "")
			current_data = current_data.replace(">", "")
			current_data = current_data.split("|")
			outlinks = current_data[0]
			outlink_num = current_data[1]
			old_pagerank = current_data[2]
			publication_year = current_data[3]
			# Add the pagerank score transfered from dangling nodes
			pagerank += dangling_pagerank
			# Calculate new pagerank value
			pagerank = alpha * pagerank + (1 - alpha) * (1/float(node_count))
			# Write output (remember to use strings concatenated by <tab> - using "," won't work on hadoop)
			print prev_key + "\t" + outlinks + "|" + outlink_num + "|" + str(pagerank) + "\t" + old_pagerank + "\t" + publication_year
			# Reset all used parameters for next key
			pagerank = 0
			# Remove the stored data from dictionary, as we wrote everything related to it
			current_data_dict.pop(prev_key, None)
			# Set the new key as the valid one
			prev_key = key
			# Add the pagerank read for the new key
			pagerank += val
		# If we were at the first key, we initialize pagerank and key/previous key values
		else: 
			prev_key = key
			pagerank += val

# Reduce may end on an unchainged key. In this case, the data for the key is not written to the
# output in the above loop. We check therefore if this is the case and print the last key's data
if(key == prev_key):
	# Do the same as in the reduce loop above
	current_data = current_data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	current_data = current_data.split("|")
	current_data_dict.pop(key, None)
	outlinks = current_data[0]
	outlink_num = current_data[1]
	old_pagerank = current_data[2]
	publication_year = current_data[3]
	pagerank += dangling_pagerank
	pagerank = alpha * pagerank + (1 - alpha) * (1/float(node_count))
	print key + "\t" + outlinks + "|" + outlink_num + "|" + str(pagerank) + "\t" + old_pagerank + "\t" + publication_year

# ------------------------------- #
