#    timeawarerank_reduce.py - Reducer script for TAR-RAM/TAR-ECM ranking
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

# Get alpha value
alpha = float(sys.argv[1])
key = "initial_key"
prev_key = "initial_prev_key"
# Initial score
timeawarerank_part = 0
# Create a dictionary containing publication data
current_data_dict = dict()

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
	# If we don't have node data, simply get rank transferred
	val = float(val)
	if(key == prev_key):
		# Sum score gathered from mappers
		timeawarerank_part  += val
	# Run this if key changes and is not the first key
	else:
		if(prev_key != "initial_prev_key"):
			# Get paper data
			current_data = current_data_dict[prev_key]
			current_data = current_data.replace("<", "")
			current_data = current_data.replace(">", "")
			current_data = current_data.split("|")
			outlinks = current_data[0]
			outlink_num = current_data[1]
			running_timeawarerank = current_data[-3]
			prev_timeawarerank = running_timeawarerank
			publication_year = current_data[-1]
			# Calculate transferred rank score
			current_timeawarerank_part = alpha * timeawarerank_part
			# Add it to the already accumulated score
			timeawarerank = float(running_timeawarerank) + float(current_timeawarerank_part)
			# Output results
			print prev_key + "\t" + outlinks + "|" + outlink_num + "|" + str(current_timeawarerank_part) + "\t" + str(timeawarerank) + "\t" + str(prev_timeawarerank) + "\t" + publication_year		
			# Reset variables
			current_data_dict.pop(prev_key, None)			
			timeawarerank_part = 0
			timeawarerank = 0
			# Start calculations for the new key
			prev_key = key
			timeawarerank_part += val
		# Run this if we are reading the first key
		else:
			# Sum scores from mappers
			timeawarerank_part += val
			prev_key = key

# Data for final key might not have been output. Do this here
if(key == prev_key):
	# Calculate as in the above loop
	current_data = current_data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	current_data = current_data.split("|")
	current_data_dict.pop(key, None)
	outlinks = current_data[0]
	outlink_num = current_data[1]
	running_timeawarerank = current_data[-3]
	prev_timeawarerank = running_timeawarerank
	publication_year = current_data[-1]
	# Calculate transferred score
	current_timeawarerank_part = alpha * timeawarerank_part
	timeawarerank = float(running_timeawarerank) + float(current_timeawarerank_part)
	# Output data
	print key + "\t" + outlinks + "|" + outlink_num + "|" + str(current_timeawarerank_part) + "\t" + str(timeawarerank) + "\t" + str(prev_timeawarerank) + "\t" + publication_year


# -------------------------------- #
			
