#    timeawarerank.py - TAR-RAM/TAR-ECM calculation script
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

# Script notifies how to correctly use, if wrong number of arguments is given
if(len(sys.argv) < 5):
	print "Usage: python timeawarerank.py <input_file> <gamma> <current_year> <mode:RAM|ECM> <optional:alpha> <optional: max_error>"
	sys.exit(0)
else:
	input_file = sys.argv[1]
	gamma = sys.argv[2]
	current_year = sys.argv[3]
	mode = sys.argv[4]
	if(mode != "ECM" and mode != "RAM"):
		print "No valid mode set. Set RAM or ECM as mode"
		exit(0)
# Default mode is RAM, so if alpha is not specified it is set to 1		
if(len(sys.argv) >= 6):
	alpha = sys.argv[5]
else:
	alpha = 1
# If error is set, ECM mode should be selected. 
# Otherwise we set a high value.
if(len(sys.argv) == 7):
	max_error = float(sys.argv[6])
else:
	max_error = 100

# On RAM mode we only need iterate once.
if(mode == "RAM"):
	max_iterations = 1
# On ECM mode the max number of iterations is equal to the number of papers given (
elif(mode == "ECM"):
	max_iterations = os.popen("wc -l " + input_file + "").read().strip().split()[0]

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #

	# max_iterations = os.popen("hadoop dfs -cat /user/hduser/" + input_file + "/* | wc -l | cut -f1 -d ' '").read().strip()

	# --------------------------- #
	# --------------------------- #

	max_iterations = int(max_iterations) - 1

# Notify user of selected parameters	
print "Gamma: ", gamma
print "Current Year: ", current_year
print "Alpha: ", alpha
print

# Create file for processing the input steps
os.popen("cp " + input_file + " graphstep.txt")

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #

# os.popen("hadoop dfs -cp /user/hduser/" + input_file + " /user/hduser/timeaware_step")

# --------------------------- #
# --------------------------- #

# Count number of iterations
num_iterations = 0

# ------------------------- #

# ---- Main Program ------- #

# Iterate the number of iterations specified
for i in range(0, max_iterations):

	# Run step
	os.popen("cat graphstep.txt | python timeawarerank_map.py " + gamma + " " + current_year + " | sort -k1,1 | python timeawarerank_reduce.py " + str(alpha) + " > nextstep.txt")
	# Turn the output file into the input of the next step
	os.popen("mv nextstep.txt graphstep.txt")

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #

	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='TimeAware Rank Iteration " + str(num_iterations) + "' -D mapred.reduce.tasks=10 -file timeawarerank_map.py -file timeawarerank_reduce.py -input /user/hduser/timeaware_step -output /user/hduser/timeaware_nextstep -mapper timeawarerank_map.py\ " + str(gamma) + "\ " + str(current_year) + " -reducer timeawarerank_reduce.py\ " + str(alpha) + "")

	# os.popen("hadoop dfs -rmr /user/hduser/timeaware_step")
	# os.popen("hadoop dfs -mv /user/hduser/timeaware_nextstep /user/hduser/timeaware_step")
	# os.popen("hadoop dfs -rmr /user/hduser/timeaware_step/_*")

	# --------------------------- #
	# --------------------------- #

	# Calculate the error
	error = float(os.popen("cat graphstep.txt | python timeawarerank_error_map.py | sort -k1,1 | python timeawarerank_error_reduce.py").read().strip())

	# --------------------------- #
	# --- HADOOP LIKE COMMAND --- #
	# --------------------------- #
	# IMPORTANT: This reducer must run on a single machine
	# os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='TimeAware Rank Error " + str(num_iterations) + "' -D mapred.reduce.tasks=1 -file timeawarerank_error_map.py -file timeawarerank_error_reduce.py -input /user/hduser/timeaware_step -output /user/hduser/timeaware_error -mapper timeawarerank_error_map.py -reducer timeawarerank_error_reduce.py")
	# error = os.popen("hadoop dfs -cat /user/hduser/timeaware_error/part*").read().strip()
	# error = float(error)
	# os.popen("hadoop dfs -rmr /user/hduser/timeaware_error")

	# --------------------------- #
	# --------------------------- #

	# Increment iteration counter
	num_iterations += 1	

	print "Iteration ", (num_iterations), "- Error: ", str(error).strip()

	# Is the error calculated is less than the specified max value, we stop iterating
	if(error < max_error):
		if(mode == "ECM"):
			print "Max error:", error, " - ending!"
		break
	
	
# Dump output in new file
os.popen("cat graphstep.txt | while IFS=$'\t' read paper dats score prevscore year; do echo  \"${paper}\t${dats}\t${score}\t${prevscore}\t${year}\t${score}\"; done | sort -k6 -gr > timeawarerank_results_file_" + input_file + "_a" + str(alpha) + "_c" + str(gamma) + "_year" + str(current_year) + "_m" + mode + ".txt")
# Cleanup various files written
os.popen("rm graphstep.txt")

# --------------------------- #
# --- HADOOP LIKE COMMAND --- #
# --------------------------- #

# os.popen("hadoop dfs -mv /user/hduser/timeaware_step /user/hduser/TAR_" + str(input_file) + "_" + str(mode) + "_c" +  str(gamma) + "_a" + str(alpha) + "_y" + str(current_year) + "_error" + str(max_error) + "")

# --------------------------- #
# --------------------------- #


# ------------------------- #

