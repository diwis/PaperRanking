#    pagerank_dangling.py - This is a simple map script to calculate 
#			  - the sum of dangling node PageRank scores
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

total_nodes = int(sys.argv[1])

# -------------------------- #

# ---- Main Program / Mapper ---- #

# Read input from stdin
for line in sys.stdin:
	# Remove leading and trailing whitespace
	line = line.strip()
	# Split based on whitespace
	keyval = line.split()
	# Get article identifier (pmid would be medline papers)
	pmid = keyval[0]
	# Split value [<cited list>|<num cited>|<pagerank score>]
	val = keyval[1].split("|")
	# Get pagerank of page
	pagerank = float(val[2])
	# Get num of outlinks
	outlinks = int(val[1])
	# Get outlink list
	outlink_list = val[0].split(",")
	# Each dangling node transfers its pagerank value to every node in the graph
	pagerank = pagerank/float(total_nodes)
	
	# Output only if we have a dangling node
	if(outlinks == 0):
		print str(1) + "\t" + str(pagerank)

# ------------------------------- #
