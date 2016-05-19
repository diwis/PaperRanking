#    pagerank.py - This script calculated PageRank scores for a paper collection.
#		   It makes us of the other map-reduce scripts in its folder. 
#		   Info on how to use can be found in the README file.
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
#    Contact Address: Institute for the Management of Information Systems, Research Center ”Athena”, Artemidos 6 & Epidavrou, Marousi 15125, Greece

# ---- Imports ---- #

import sys
import os

# ----------------- #

# ---- Initialisations ---- #

node_count = 0
filename = ""
start_year = ""

# ------------------------- #

# ---- Configure PageRank - Read arguments and prepare iterations ---- #

# Read filename as argument
if(len(sys.argv) < 4):
	print "Usage: ./pagerank <filename> <alpha> <convergence_error>"
	print
	sys.exit()
else:
	# Get input filename
	filename = sys.argv[1]
	# Calculate total number of nodes
	node_count = os.popen("wc -l " + filename + " | cut -f1 -d ' '").read()
	node_count = int(node_count)
	alpha = float(sys.argv[2])
	convergence_error = float(sys.argv[3])

# Set an initial (high) delta (max error) variable!
delta = 100
# Print out pagerank calculation data
print "Nodes: ", node_count
print "Alpha: ", alpha
print "Initial delta: ", delta
print "File: ", filename
print

# ---------------------------------------------------------------------#

# ------------------------------------------------------------------------------------------- #
# ---- Main Program: Run PageRank steps until the error converges to the specified value ---- #
# ---- Steps are in their order: 							 ---- #
# ---- 		(a) Calculate dangling sum, 						 ---- #
# ---- 		(b) run pagerank step, 							 ---- #
# ---- 		(c) calculate error							 ---- #
# ------------------------------------------------------------------------------------------- #

# A. Initial step!

# 1. Get dangling node pagerank sum - if a start year had been specified, we need only do this for dangling nodes
dangling_sum = os.popen("cat " + filename + " | ./pagerank_dangling.py " + str(node_count) + " " + str(start_year) + " | sort -k1,1 | ./pagerank_dangling_reducer.py").read()
# Convert dnagling_sum string to float
dangling_sum = float(dangling_sum)
# 2. Execute pagerank step
os.popen("cat " + filename + " | ./pagerank_map.py " + start_year + " | sort -k1,1 | ./pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " > graphstep.txt")
# 3. Calculate Error
delta = os.popen("cat graphstep.txt | ./map_error.py | ./reduce_error.py").read()
# Convert error string to Float
delta = float(delta)

# Count the above as a first iteration
iterations = 1
print "Iteration " + str(iterations) + " - Max error: ", delta

# B. Start iterating the above
while(delta >= convergence_error):
	
	# 1. Get dangling node pagerank sum
	dangling_sum = os.popen("cat graphstep.txt | ./pagerank_dangling.py " + str(node_count) + " " + str(start_year) + " | sort -k1,1 | ./pagerank_dangling_reducer.py").read()
	# Convert dnagling_sum string to float
	dangling_sum = float(dangling_sum)
	# 2. Execute pagerank step
	os.popen("cat graphstep.txt | ./pagerank_map.py | sort -k1,1 | ./pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " > nextstep.txt")
	# 3. Calculate Error
	delta = os.popen("cat nextstep.txt | ./map_error.py | ./reduce_error.py").read()
	# Convert error string to Float
	delta = float(delta)
	iterations += 1
	print "Iteration " + str(iterations) + " - Max error: ", delta
	# Replace the file processed in this step with the one generated
	os.popen("mv nextstep.txt graphstep.txt")

# PageRank calculation has finished here!

# Output total number of iterations
print
print "Total iterations: ", iterations

# Normalize scores!

# Sum the scores of all papers
pagerank_sum = os.popen("cat graphstep.txt | ./sum_pr_map.py |  ./sum_pr_reduce.py ").read()
pagerank_sum = float(pagerank_sum)
normalise_val = 1/pagerank_sum
# Calculate normalised PageRank scores
os.popen("cat graphstep.txt | ./normalise_pr_map.py " + str(normalise_val) + " > nextstep.txt")	
# Sort final results based on the pagerank scores and output results into final file
os.popen("cat nextstep.txt | sort -g -r -k3 > final_pagerank_" + start_year + filename.replace(".txt", "") + "_" + str(alpha) + "_" + str(convergence_error) + ".txt")
# Remove all intermediate files step files
os.popen("rm nextstep.txt graphstep.txt")

# ------------------------------------------------------------------------------------------- #
