#    timeawarerank_map.py - Mapper script for TAR-RAM/TAR-ECM ranking
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

# Mapper needs value of gamma and current year value
gamma = float(sys.argv[1])
current_year = int(sys.argv[2])

# ------------------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove leading and trailing whitespace
	line = line.strip()
	# Split based on whitespace
	line_parts = line.split()
	# This means that we will have initial pagerank-like input.
	# (This is because for all ranking methods we use the same input format
	if(len(line_parts) < 5):
		# Get year of publication
		publication_year = line_parts[-1]
		# Get pmid
		pmid = line_parts[0]
		# Split value
		pub_data = line_parts[1].split("|")
		# Set initial TAR score value
		timeawarerank = 0
		# Get num of outlinks
		outlinks = int(pub_data[1])
		# Get outlink list
		outlink_list = pub_data[0].split(",")
		# Running sum 
		timeawarerank_part = gamma ** (int(current_year) - int(publication_year))
		# Previous running sum
		prev_timeawarerank = timeawarerank
	# This runs in any iteration that is not the first (for ECM)
	else:
		# Get year of publication
		publication_year = line_parts[-1]
		# Get pmid
		pmid = line_parts[0]
		# Split value
		pub_data = line_parts[1].split("|")
		# Get num of outlinks
		outlinks = int(pub_data[1])
		# Get outlink list
		outlink_list = pub_data[0].split(",")
		# Running sum
		timeawarerank = line_parts[-3]
		# Previous running sum
		prev_timeawarerank = line_parts[-2]	
		# Previous timeaware rank part
		prev_timeawarerank_part = pub_data[-1]	
		# Get citerank of page
		timeawarerank_part = gamma ** (int(current_year) - int(publication_year)) * float(prev_timeawarerank_part) 
	
	# Output self first to ensure the key is found in the reduce phase
	print pmid + "\t" + "0"
	# Output previous data
	print pmid + "\t" + "<" + pub_data[0] + "|" + str(outlinks) + "|" + str(timeawarerank_part) + "|" + str(timeawarerank) + "|" + str(prev_timeawarerank) + "|" + publication_year + ">"

	# Loop cited papers and output corresponding key - values
	for outlink in outlink_list:
		if(outlinks != 0 and float(timeawarerank_part) != 0.0):
			# In this method we do not divide the rank transferred by the number of outlinks
			print outlink + "\t" + str(timeawarerank_part)

# ------------------------------- #	

