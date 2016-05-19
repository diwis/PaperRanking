#    citerank_map.py - This script is the mapper for a citerank iteration.
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

# Read CR parameters from input
tdir = float(sys.argv[1])
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
	# (initial input is common for all ranking methods - files produced in each method's iteration differ)
	if(len(line_parts) < 5):
		# Get year of publication
		publication_year = line_parts[-1]
		# Get pmid
		pmid = line_parts[0]
		# Split value
		pub_data = line_parts[1].split("|")
		# Get citerank of page
		citerank_part = math.exp(-(int(current_year) - int(publication_year))/float(tdir))
		# Get num of outlinks
		outlinks = int(pub_data[1])
		# Get outlink list
		outlink_list = pub_data[0].split(",")
		# Running sum
		citerank = math.exp(-(int(current_year) - int(publication_year))/float(tdir))
		# Previous running sum
		prev_citerank = citerank
	# This should run on any iteration, but the first
	else:
		# Get year of publication
		publication_year = line_parts[-1]
		# Get pmid
		pmid = line_parts[0]
		# Split value
		pub_data = line_parts[1].split("|")
		# Get citerank of page
		citerank_part = float(pub_data[2])
		# Get num of outlinks
		outlinks = int(pub_data[1])
		# Get outlink list
		outlink_list = pub_data[0].split(",")
		# Running sum
		citerank = line_parts[-3]
		# Previous running sum
		prev_citerank = line_parts[-2]		
	
	# Output self first - ensure that the key will be found in the reduce phase
	print pmid + "\t" + str(0)
	# Output pmid previous data
	print pmid + "\t" + "<" + pub_data[0] + "|" + str(outlinks) + "|" + str(citerank_part) + "|" + str(citerank) + "|" + str(prev_citerank) + "|" + publication_year + ">"
	
	# Loop cited papers and output corresponding key - values with citerank scores that are transferred
	for outlink in outlink_list:
		if(outlinks != 0):
			print outlink + "\t" + str(citerank_part/float(outlinks))

# ------------------------------- #	

