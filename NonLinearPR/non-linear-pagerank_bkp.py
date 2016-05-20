#    non-linear-pagerank.py - script that implements NLPR calculation
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
import os
import time

# ----------------- #

# ---- Initialisations ---- #

node_count = 0
filename = ""

# Inform of correct script usage
if(len(sys.argv) < 5):
	print "Usage: ./non-linear-pagerank.py <filename> <alpha> <convergence_error> <theta>"
	print
	sys.exit()
# Read script/method params from command line
else:
	# Get input filename
	filename = sys.argv[1]
	# Calculate total number of nodes
	node_count = os.popen("wc -l " + filename + " | cut -f1 -d ' '").read()
	node_count = int(node_count)
	# Read value for alpha
	alpha = float(sys.argv[2])
	# Read convergence error
	convergence_error = float(sys.argv[3])
	# Read theta
	theta = sys.argv[4]

# Set an initial (high) delta value!
delta = 100

# Inform of params/settings
print "Nodes: ", node_count
print "Alpha: ", alpha
print "Theta: ", theta
print "Initial delta: ", delta
print "File: ", filename

# ------------------------- #

# ---- Do an initial calcualtion step ----- #

# 1. Get dangling node rank sum
dangling_sum = os.popen("cat " + filename + " | ./nl_pagerank_dangling.py " + str(node_count) + " | sort -k1,1 | ./nl_pagerank_dangling_reducer.py").read()
# Convert sum to float
dangling_sum = float(dangling_sum)

# 2. Execute non-linear pagerank step
os.popen("cat " + filename + " | ./nl_pagerank_map.py | sort -k1,1 | ./nl_pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " " + str(theta) + " > graphstep.txt")

# 3. Calculate max error
delta = os.popen("cat graphstep.txt | ./nl_map_error.py | ./nl_reduce_error.py").read()
# Convert to float
delta = float(delta)

# Count iterations
iterations = 1
print "Iteration: " + str(iterations) + " - Max error: ", delta

# Set output of step as input of next step
os.popen("mv nextstep.txt graphstep.txt")

# Repeat until convergence
while(delta >= convergence_error):

	# 1. Get dangling node rank sum
	dangling_sum = os.popen("cat graphstep.txt | ./nl_pagerank_dangling.py " + str(node_count) + " | sort -k1,1 | ./nl_pagerank_dangling_reducer.py").read()
	# Convert sum to float
	dangling_sum = float(dangling_sum)

	# 2. Execute pagerank step
	os.popen("cat graphstep.txt | ./nl_pagerank_map.py | sort -k1,1 | ./nl_pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " " + str(theta) + " > nextstep.txt")

	# 3. Calculate max error
	delta = os.popen("cat nextstep.txt | ./nl_map_error.py | ./nl_reduce_error.py").read().strip()
	delta = float(delta)

	# Turn current output file to next step's input file
	os.popen("mv nextstep.txt graphstep.txt")

	iterations += 1
	print "Iteration " + str(iterations) + " - Max error: ", delta	

print "Total iterations: ", iterations

# Normalise scores
normalise_values = os.popen("cat graphstep.txt | ./sum_pr_map.py |  ./sum_pr_reduce.py ").read()
normalisers = normalise_values.split()
pagerank_sum = float(normalisers[0])
prev_pr_sum = float(normalisers[1])

normalise_val = 1/pagerank_sum
prev_normalise_val = 1/prev_pr_sum

# Calculate normalised PageRank values
os.popen("cat graphstep.txt | ./normalise_pr_map.py " + str(normalise_val) + " " + str(prev_normalise_val) + " > nextstep.txt")	
# Output final file
os.popen("cat nextstep.txt | sort -g -r -k3 > final_nl_pagerank_"  + filename.replace(".txt", "") + "_" + str(alpha) + "_" + str(theta) + "_" + str(convergence_error) + ".txt")

# Remove all other created files
os.popen("rm nextstep.txt graphstep.txt")
