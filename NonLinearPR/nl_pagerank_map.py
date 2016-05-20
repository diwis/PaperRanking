#    nl_pagerank_map.py - NLPR iteration step mapper script
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

# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove leading and trailing whitespace
	line = line.strip()
	# Split based on whitespace
	keyval = line.split()
	# Get year of publication
	publication_year = keyval[3]
	# Get pmid
	pmid = keyval[0]
	# Split value
	val = keyval[1].split("|")
	# Get pagerank of page
	pagerank = float(val[2])
	# Get num of outlinks
	outlinks = int(val[1])
	# Get outlink list
	outlink_list = val[0].split(",")
	
	# Output data for current paper - ensure it appears in the reduce phase
	print pmid + "\t" + "0"
	# Output papers current data
	print pmid + "\t" + "<" + val[0] + "|" + str(outlinks) + "|" + str(pagerank) + "|" + publication_year + ">"

	# Loop cited papers and output corresponding key - values
	for outlink in outlink_list:
		if(outlinks != 0):
			print outlink + "\t" + str(pagerank/float(outlinks))
	
# ------------------------------- #
