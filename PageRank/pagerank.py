#    pagerank.py - This script calculated PageRank scores for a paper collection.
#		   It makes us of the other map-reduce scripts in its folder. 
#		   Info on how to use can be found in the README file.
#
#    Copyright (C) 2016  IMIS, Athena RC, Ilias Kanellos
#
#    This program is free software: you can redistribute it anpython or modify
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
#    along with this program.  If not, see <httppython /www.gnu.orpython licensepython >.
#
#    Contact email: ilias.kanellos@imis.athena-innovation.gr
#    Contact Address: Institute for the Management of Information Systems, Research Center "Athena", Artemidos 6 & Epidavrou, Marousi 15125, Greece

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
	print "Usage: python pagerank <filename> <alpha> <convergence_error>"
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
dangling_sum = os.popen("cat " + filename + " | python pagerank_dangling.py " + str(node_count) + " " + str(start_year) + " | sort -k1,1 | python pagerank_dangling_reducer.py").read()
# Convert dnagling_sum string to float
dangling_sum = float(dangling_sum)

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Initial Dangling Sum Step' -D mapred.reduce.tasks=1 -file pagerank_dangling.py -file pagerank_dangling_reducer.py -input /user/hduser/" + filename + " -output dangling_output -mapper pagerank_dangling.py\ " + str(node_count) + " -reducer pagerank_dangling_reducer.py")
# dangling_sum = os.popen("hadoop dfs -cat /user/hduser/dangling_output/part*").read().strip()
# Remove old dangling file
# os.popen("hadoop dfs -rmr /user/hduser/dangling_output")

# ------------------------- #
# ------------------------- #


# 2. Execute pagerank step
os.popen("cat " + filename + " | python pagerank_map.py " + start_year + " | sort -k1,1 | python pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " > graphstep.txt")

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Initial Dangling Sum " + filename + "' -D mapred.reduce.tasks=10 -file pagerank_map.py -file pagerank_reducer.py -input /user/hduser/" + filename + " -output /user/hduser/pagerank_step -mapper pagerank_map.py -reducer pagerank_reducer.py\ " + str(alpha) + "\ " + str(dangling_sum) + "\ " + str(node_count))

# ------------------------- #
# ------------------------- #

# 3. Calculate Error
delta = os.popen("cat graphstep.txt | python map_error.py | python reduce_error.py").read()
# Convert error string to Float
delta = float(delta)

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Initial PageRank Error " + filename + "' -D mapred.reduce.tasks=1 -file map_error.py -file reduce_error.py -input /user/hduser/pagerank_step -output /user/hduser/pagerank_error -mapper map_error.py -reducer reduce_error.py")
# delta = os.popen("hadoop dfs -cat  /user/hduser/pagerank_error/part*").read().strip()
# delta = float(delta)
# Remove error dir
# os.popen("hadoop dfs -rmr /user/hduser/pagerank_error")

# ------------------------- #
# ------------------------- #


# Count the above as a first iteration
iterations = 1
print "Iteration " + str(iterations) + " - Max error: ", delta

# B. Start iterating the above
while(delta >= convergence_error):
	
	# 1. Get dangling node pagerank sum
	dangling_sum = os.popen("cat graphstep.txt | python pagerank_dangling.py " + str(node_count) + " " + str(start_year) + " | sort -k1,1 | python pagerank_dangling_reducer.py").read()
	# Convert dnagling_sum string to float
	dangling_sum = float(dangling_sum)

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #	
	# IMPORTANT: This should run on a SINGLE reducer #

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Dangling Iteration " + str(iterations) + " " + filename + "' -D mapred.reduce.tasks=1 -file pagerank_dangling_reducer.py -input /user/hduser/pagerank_step -output dangling_output -mapper pagerank_dangling.py\ " + str(node_count) + " -reducer pagerank_dangling_reducer.py")
        # dangling_sum = os.popen("hadoop dfs -cat /user/hduser/dangling_output/part*").read().strip()
        # Remove old dangling file
        # os.popen("hadoop dfs -rmr /user/hduser/dangling_output")
        # dangling_sum = float(dangling_sum)

	# ------------------------- #
	# ------------------------- #

	# 2. Execute pagerank step
	os.popen("cat graphstep.txt | python pagerank_map.py | sort -k1,1 | python pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " > nextstep.txt")

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #
	
        # os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='PageRank Iteration " + str(iterations) + " " + filename + "' -D mapred.reduce.tasks=10 -file pagerank_map.py -file pagerank_reducer.py -input /user/hduser/pagerank_step -output /user/hduser/pagerank_nextstep -mapper pagerank_map.py -reducer pagerank_reducer.py\ " + str(alpha) + "\ " + str(dangling_sum) + "\ " + str(node_count))
        # Remove step before
        # os.popen("hadoop dfs -rmr /user/hduser/pagerank_step")

	# ------------------------- #
	# ------------------------- #

	# 3. Calculate Error
	delta = os.popen("cat nextstep.txt | python map_error.py | python reduce_error.py").read()
	# Convert error string to Float
	delta = float(delta)

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #
	# NOTE: Run on SINGLE reducer!
        # os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='PageRank Error Iteration " + str(iterations) +" " + filename + "' -D mapred.reduce.tasks=1 -file map_error.py -file reduce_error.py -input /user/hduser/pagerank_nextstep -output /user/hduser/pagerank_error -mapper map_error.py -reducer reduce_error.py")
        # delta = os.popen("hadoop dfs -cat  /user/hduser/pagerank_error/part*").read().strip()
        # delta = float(delta)
        # Remove error dir
        # os.popen("hadoop dfs -rmr /user/hduser/pagerank_error")
	
	# ------------------------- #
	# ------------------------- #


	iterations += 1
	print "Iteration " + str(iterations) + " - Max error: ", delta
	# Replace the file processed in this step with the one generated
	os.popen("mv nextstep.txt graphstep.txt")

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #

        # os.popen("hadoop dfs -mv /user/hduser/pagerank_nextstep /user/hduser/pagerank_step")
        # os.popen("hadoop dfs -rmr /user/hduser/pagerank_step/_*")

	# ------------------------- #
	# ------------------------- #

# PageRank calculation has finished here!

# Output total number of iterations
print
print "Total iterations: ", iterations

# Normalize scores!

# Sum the scores of all papers
pagerank_sum = os.popen("cat graphstep.txt | python sum_pr_map.py |  python sum_pr_reduce.py ").read()
pagerank_sum = float(pagerank_sum)
normalise_val = 1/pagerank_sum

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='PageRank Sum Calculation " + filename + "' -D mapred.reduce.tasks=1 -file sum_pr_map.py -file sum_pr_reduce.py -input /user/hduser/pagerank_step -output /user/hduser/pagerank_sum -mapper sum_pr_map.py -reducer sum_pr_reduce.py")
# pagerank_sum = os.popen("hadoop dfs -cat /user/hduser/pagerank_sum/part*").read().strip()
# pagerank_sum = float(pagerank_sum)
# normalise_val = 1/pagerank_sum

# ------------------------- #
# ------------------------- #

# Calculate normalised PageRank scores
os.popen("cat graphstep.txt | python normalise_pr_map.py " + str(normalise_val) + " > nextstep.txt")	
# Sort final results based on the pagerank scores and output results into final file
os.popen("cat nextstep.txt | sort -g -r -k3 > final_pagerank_" + start_year + filename.replace(".txt", "") + "_" + str(alpha) + "_" + str(convergence_error) + ".txt")

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='PageRank Normalisation " + filename + "' -D mapred.reduce.tasks=0 -file normalise_pr_map.py -input /user/hduser/pagerank_step -output /user/hduser/PR_" + filename + "_" + str(alpha) + "_" + str(convergence_error) + " -mapper normalise_pr_map.py\ " + str(normalise_val)  + "")

# ------------------------- #
# ------------------------- #

# Remove all intermediate files step files
os.popen("rm nextstep.txt graphstep.txt")

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("hadoop dfs -rmr /user/hduser/pagerank_step /user/hduser/pagerank_sum")

# ------------------------- #
# ------------------------- #



# ------------------------------------------------------------------------------------------- #
