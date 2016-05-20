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
	print "Usage: python non-linear-pagerank.py <filename> <alpha> <convergence_error> <theta>"
	print
	sys.exit()
# Read script/method params from command line
else:
	# Get input filename
	filename = sys.argv[1]
	# Calculate total number of nodes
	node_count = os.popen("wc -l " + filename + " | cut -f1 -d ' '").read()

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #

	# node_count = os.popen("hadoop dfs -cat /user/hduser/" + filename + "/* | wc -l | cut -f1 -d ' '").read().strip()

	# --------------------------- #
	# --------------------------- #

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
dangling_sum = os.popen("cat " + filename + " | python nl_pagerank_dangling.py " + str(node_count) + " | sort -k1,1 | python nl_pagerank_dangling_reducer.py").read()

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #
# Important: use a SINGLE REDUCER here

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Initial NlPageRank Dangling Step' -D mapred.reduce.tasks=1 -file nl_pagerank_dangling.py -file nl_pagerank_dangling_reducer.py -input /user/hduser/" + filename + " -output /user/hduser/nl_dangling_output -mapper nl_pagerank_dangling.py\ " + str(node_count) + " -reducer nl_pagerank_dangling_reducer.py")
# dangling_sum = os.popen("hadoop dfs -cat /user/hduser/nl_dangling_output/part*").read().strip()
# Remove old dangling file
# os.popen("hadoop dfs -rmr /user/hduser/nl_dangling_output")

# --------------------------- #
# --------------------------- #


# Convert sum to float
dangling_sum = float(dangling_sum)

# 2. Execute non-linear pagerank step
os.popen("cat " + filename + " | python nl_pagerank_map.py | sort -k1,1 | python nl_pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " " + str(theta) + " > graphstep.txt")

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='First NlPageRank Step' -D mapred.reduce.tasks=10 -file nl_pagerank_map.py -file nl_pagerank_reducer.py -input /user/hduser/" + filename + " -output /user/hduser/nl_pagerank_step -mapper nl_pagerank_map.py -reducer nl_pagerank_reducer.py\ " + str(alpha) + "\ " + str(dangling_sum) + "\ " + str(node_count)  + "\ " + str(theta) + "")

# --------------------------- #
# --------------------------- #

# 3. Calculate max error
delta = os.popen("cat graphstep.txt | python nl_map_error.py | python nl_reduce_error.py").read()

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #
# Important: Use a SINGLE REDUCER HERE

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Initial NlPageRank Error " + filename + "' -D mapred.reduce.tasks=1 -file nl_map_error.py -file nl_reduce_error.py -input /user/hduser/nl_pagerank_step -output /user/hduser/nl_pagerank_error -mapper nl_map_error.py -reducer nl_reduce_error.py")
# delta = os.popen("hadoop dfs -cat  /user/hduser/nl_pagerank_error/part*").read().strip()

# --------------------------- #
# --------------------------- #

# Convert to float
delta = float(delta)

# Count iterations
iterations = 1
print "Iteration: " + str(iterations) + " - Max error: ", delta

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #

# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_error")

# --------------------------- #
# --------------------------- #

# Repeat until convergence
while(delta >= convergence_error):

	# 1. Get dangling node rank sum
	dangling_sum = os.popen("cat graphstep.txt | python nl_pagerank_dangling.py " + str(node_count) + " | sort -k1,1 | python nl_pagerank_dangling_reducer.py").read()

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #
	# Important: Use SINGLE REDUCER here

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NlPageRank Dangling Iteration " + str(iterations) + " " + filename + "' -D mapred.reduce.tasks=1 -file nl_pagerank_dangling.py -file nl_pagerank_dangling_reducer.py -input /user/hduser/nl_pagerank_step -output /user/hduser/nl_dangling_output -mapper nl_pagerank_dangling.py\ " + str(node_count) + " -reducer nl_pagerank_dangling_reducer.py")
	# dangling_sum = os.popen("hadoop dfs -cat /user/hduser/nl_dangling_output/part*").read().strip()
	# Remove old dangling file
	# os.popen("hadoop dfs -rmr /user/hduser/nl_dangling_output")

	# --------------------------- #
	# --------------------------- #

	# Convert sum to float
	dangling_sum = float(dangling_sum)

	# 2. Execute pagerank step
	os.popen("cat graphstep.txt | python nl_pagerank_map.py | sort -k1,1 | python nl_pagerank_reducer.py " + str(alpha) + " " + str(dangling_sum) + " " + str(node_count) + " " + str(theta) + " > nextstep.txt")

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NlPageRank Iteration " + str(iterations) + " " + filename + "' -D mapred.reduce.tasks=10 -file nl_pagerank_map.py -file nl_pagerank_reducer.py -input /user/hduser/nl_pagerank_step -output /user/hduser/nl_pagerank_nextstep -mapper nl_pagerank_map.py -reducer nl_pagerank_reducer.py\ " + str(alpha) + "\ " + str(dangling_sum) + "\ " + str(node_count) + "\ " + str(theta) + "")
	# Remove step before
	# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_step")

	# --------------------------- #
	# --------------------------- #

	# 3. Calculate max error
	delta = os.popen("cat nextstep.txt | python nl_map_error.py | python nl_reduce_error.py").read().strip()

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #
	# Important: use SINGLE reducer

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NlPageRank Error Iteration " + str(iterations) +" " + filename + "' -D mapred.reduce.tasks=1 -file nl_map_error.py -file nl_reduce_error.py -input /user/hduser/nl_pagerank_nextstep -output /user/hduser/nl_pagerank_error -mapper nl_map_error.py -reducer nl_reduce_error.py")
	# delta = os.popen("hadoop dfs -cat  /user/hduser/nl_pagerank_error/part*").read().strip()

	# --------------------------- #
	# --------------------------- #

	delta = float(delta)

	# Turn current output file to next step's input file
	os.popen("mv nextstep.txt graphstep.txt")

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #

	# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_error")
	# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_nextstep")
	# os.popen("hadoop dfs -mv /user/hduser/nl_pagerank_nextstep /user/hduser/nl_pagerank_step")
	# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_step/_*")

	# --------------------------- #
	# --------------------------- #
	iterations += 1
	print "Iteration " + str(iterations) + " - Max error: ", str(delta).strip()	

print "Total iterations: ", iterations

# Normalise scores
normalise_values = os.popen("cat graphstep.txt | python sum_pr_map.py |  python sum_pr_reduce.py ").read()

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #
# Important: Use SINGLE reducer

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NlPageRank Normalization Coefficients " + str(iterations) + " " + filename + "' -D mapred.reduce.tasks=1 -file sum_pr_map.py -file sum_pr_reduce.py -input /user/hduser/nl_pagerank_step -output /user/hduser/nl_pagerank_normalizers -mapper sum_pr_map.py -reducer sum_pr_reduce.py")
# normalise_values = os.popen("hadoop dfs -cat /user/hduser/nl_pagerank_normalizers/part*").read().strip()

# --------------------------- #
# --------------------------- #

normalisers = normalise_values.split()
pagerank_sum = float(normalisers[0])
prev_pr_sum = float(normalisers[1])

normalise_val = 1/pagerank_sum
prev_normalise_val = 1/prev_pr_sum

# Calculate normalised PageRank values
os.popen("cat graphstep.txt | python normalise_pr_map.py " + str(normalise_val) + " " + str(prev_normalise_val) + " > nextstep.txt")	
# Output final file
os.popen("cat nextstep.txt | sort -g -r -k3 > final_nl_pagerank_"  + filename.replace(".txt", "") + "_" + str(alpha) + "_" + str(theta) + "_" + str(convergence_error) + ".txt")

# Remove all other created files
os.popen("rm nextstep.txt graphstep.txt")

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #

# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NlPageRank Normalisation " + filename + "' -D mapred.reduce.tasks=0 -file normalise_pr_map.py -input /user/hduser/nl_pagerank_step -output /user/hduser/NLPR_" + filename + "_" + str(alpha) + "_" + str(convergence_error) + "_" + str(theta) + "_i" + str(iterations) + " -mapper normalise_pr_map.py\ " + str(normalise_val) + "\ " + str(prev_normalise_val) + "")

# os.popen("hadoop dfs -rmr /user/hduser/nl_pagerank_step /user/hduser/nl_pagerank_normalizers")

# --------------------------- #
# --------------------------- #

