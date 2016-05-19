#    citerank.py - Program implementing CR iterations
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

# ----------------- #

# ---- Initialisations ---- #

# If we don't supply the correct arguments, the script notifies the user how to run it
if(len(sys.argv) < 6):
	print "Usage: python citerank.py <input_file> <alpha> <tdir> <current_year> <convergence_error> <optional:max_iterations>"
	sys.exit(0)
else:
	input_file = sys.argv[1]
	alpha = sys.argv[2]
	tdir = sys.argv[3]
	current_year = sys.argv[4]
	max_error = sys.argv[5]
# We can either set the maximum number of iterations, or rely on convergence
try:
	max_iterations = int(sys.argv[6])
except:
	max_iterations = None
	
# Inform user of settings
print "Alpha: ", alpha
print "Tdir: ", tdir
print "Current Year: ", current_year
print "Convergence on: ", max_error
print
# Create file for processing the input steps
os.popen("cp " + input_file + " graphstep.txt")

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("hadoop dfs -cp /user/hduser/" + input_file + " /user/hduser/citerank_graphstep")

# ------------------------- #
# ------------------------- #

# Count number of iterations
num_iterations = 0
# Set a fake (high) initial error
delta = 100

# ------------------------- #

# ---- Main Program ---- #

# Iterate until CR converges
while(delta >= float(max_error)):

	# Num of iterations will be taken into account when calculating
	os.popen("cat graphstep.txt | python citerank_map.py " + tdir + " " + current_year + " | sort -k1,1 | python citerank_reduce.py " + alpha + " > nextstep.txt")

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='CiteRank Iteration " + str(num_iterations) + "' -D mapred.reduce.tasks=10 -file citerank_map.py -file citerank_reduce.py -input /user/hduser/citerank_graphstep -output /user/hduser/citerank_nextstep -mapper citerank_map.py\ " + str(tdir) + "\ " + str(current_year) + " -reducer citerank_reduce.py\ " + str(alpha) + "")

	# ------------------------- #
	# ------------------------- #

	# Get the error value
	delta = os.popen("cat nextstep.txt | python citerank_error_map.py | sort -k1,1 | python citerank_error_reduce.py").read()
	delta = delta.strip()
	delta = float(delta)

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #
	# NOTE: This step should run with a SINGLE REDUCER

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='CiteRank Error Calculation " + str(num_iterations) + "' -D mapred.reduce.tasks=1 -file citerank_error_map.py -file citerank_error_reduce.py -input /user/hduser/citerank_nextstep -output /user/hduser/citerank_error -mapper citerank_error_map.py -reducer citerank_error_reduce.py")
	# delta = os.popen("hadoop dfs -cat /user/hduser/citerank_error/part*").read().strip()
	# delta = float(delta)
	# Remove error dir - remove previous step dir
	# os.popen("hadoop dfs -rmr /user/hduser/citerank_error /user/hduser/citerank_graphstep")

	# ------------------------- #
	# ------------------------- #

	print "Iteration: ", str(num_iterations), " - Max error: ", delta

	# Increment iteration counter
	num_iterations += 1

	# Set the file output of previous step as input of next
	os.popen("mv nextstep.txt graphstep.txt")

	# ------------------------- #
	# -- HADOOP LIKE COMMAND -- #
	# ------------------------- #

	# os.popen("hadoop dfs -mv /user/hduser/citerank_nextstep /user/hduser/citerank_graphstep")
	# os.popen("hadoop dfs -rmr /user/hduser/citerank_graphstep/_*")

	# ------------------------- #
	# ------------------------- #

	# If we specified an upper limit for the iterations
	# and it has been reached, we stop.
	if(max_iterations and num_iterations >= max_iterations):
		break
	
# Dump final output in new fil
os.popen("cat graphstep.txt | while IFS=$'\t' read paper dats score prevscore year; do echo  \"${paper}\t${dats}\t${score}\t${prevscore}\t${year}\t${score}\"; done | sort -k6 -gr > citerank_results_file_" + input_file + "_a" + str(alpha) + "_tdir" + str(tdir) + "_year" + str(current_year) + "_i" + str(num_iterations) + "_error" + str(delta) + ".txt")

# ------------------------- #
# -- HADOOP LIKE COMMAND -- #
# ------------------------- #

# os.popen("hadoop dfs -mv /user/hduser/citerank_graphstep /user/hduser/CR_" + input_file + "_a" + str(alpha) + "_year" + str(current_year) + "_tdir" +  str(tdir) + "_error" + str(max_error) + "_i" + str(num_iterations) + "")

# ------------------------- #
# ------------------------- #

# Remove any files created during computation
os.popen("rm graphstep.txt")

# ---------------------- #


