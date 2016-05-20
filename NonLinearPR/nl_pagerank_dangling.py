#    nl_pagerank_dangling.py - NLPR mapper for dangling node score transfer calculation
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

# ---- Initialisations ----- #

# Number of nodes in the citation graph is passed from command line
total_nodes = int(sys.argv[1])
# Accumulator for dangling node score sum
partial_dangling_sum = 0
# Num to divide dangling sum score with
divisor = float(1)/float(total_nodes)

# -------------------------- #


# ---- Main Program / Mapper ---- #

# Read input line by line
for line in sys.stdin:
	# Remove leading and trailing whitespace
	line = line.strip()
	# Split based on whitespace
	keyval = line.split()
	# Get pmid
	pmid = keyval[0]
	# Split value
	val = keyval[1].split("|")
	# Get num of outlinks
	outlinks = int(val[1])
	# Proces only data for papers that have no outgoing links
	if(outlinks != 0):
		continue
	# Get pagerank of page
	pagerank = float(val[2])
	# Each dangling node transfers its pagerank value to each node in the graph
	pagerank = pagerank * divisor
	partial_dangling_sum += pagerank

# Output partial score from dangling nodes
print str(1) + "\t" + str(float(partial_dangling_sum))

# ------------------------------- #
