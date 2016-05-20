#    nl_pagerank_map.py - NLPR iteration step reducer script
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

key = "initial_key"
prev_key = "initial_prev_key"
pagerank = 0
current_data = ""
# Create a dictionary containing publication data
current_data_dict = dict()
# Get dangling node pagerank sum as an argument
alpha = float(sys.argv[1])
dangling_pagerank = float(sys.argv[2])
node_count = int(sys.argv[3])
theta = float(sys.argv[4])

# ------------------------- #

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
	# If we don't have node data, simply get the nl-pagerank
	val = float(val)
	# Get key
	if(key == prev_key):
		# Add partial score to already calculated one
		powered = math.pow(float(val), float(theta+1))
		pagerank += powered
	else:
	# This will run if we have a key change
		if(prev_key != "initial_prev_key"):
			# Read data 
			current_data = current_data_dict[prev_key]		
			current_data = current_data.replace("<", "")
			current_data = current_data.replace(">", "")
			current_data = current_data.split("|")
			outlinks = current_data[0]
			outlink_num = current_data[1]
			old_pagerank = current_data[2]
			publication_year = current_data[3]
			# Get the (theta+1) root of the nl-pagerank added so far
			pagerank = math.pow(pagerank, float(1)/float(theta+1))
			pagerank += dangling_pagerank
			pagerank = alpha * pagerank + (1 - alpha) 
			# Output caluclated new score
			print prev_key + "\t" + outlinks + "|" + outlink_num + "|" + str(pagerank) + "\t" + old_pagerank + "\t" + publication_year
			# Reset variables
			current_data_dict.pop(prev_key, None)
			pagerank = 0
			# Set new key and start calculations for it
			prev_key = key
			powered = math.pow(float(val), float(theta+1))
			pagerank += powered
		# This will run on the initial key
		else:
			prev_key = key
			powered = math.pow(float(val), float(theta+1))
			pagerank += powered

# For the last key read, we may not have output any results.
# We do this now
if(key == prev_key):
	current_data = current_data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	current_data = current_data.split("|")
	current_data_dict.pop(key, None)
	outlinks = current_data[0]
	outlink_num = current_data[1]
	old_pagerank = current_data[2]
	publication_year = current_data[3]
	pagerank = math.pow(pagerank, float(1)/float(theta+1))
	pagerank += dangling_pagerank
	pagerank = alpha * pagerank + (1 - alpha) 
	print key + "\t" + outlinks + "|" + outlink_num + "|" + str(pagerank) + "\t" + old_pagerank + "\t" + publication_year

# -------------------------------- #
