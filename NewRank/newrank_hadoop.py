#    Copyright (C) 2018  IMIS, Athena RC, Ilias Kanellos
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

import sys
import os
import time

# -------------------------------------------------------------------- #

# Report usage
if (len(sys.argv) < 4): 
	print "Usage: "
	print "./newrank_hadoop.py <input_file> <alpha> <convergence_error> <optional: max_iterations>"
	sys.exit(0)

# Read arguments
input_file 			= sys.argv[1]
alpha      			= sys.argv[2]
convergence_error	= float(sys.argv[3])
try:
	max_iterations  = int(sys.argv[4])
except: 
	max_iterations  = ""

# Signle node
# node_number = os.popen('wc -l ' + input_file + ' | cut -f 1 -d " " ').read().strip()
# Multi node
node_number = os.popen("hadoop dfs -cat /user/hduser/" + input_file + "/* | wc -l | cut -d ' ' -f 1").read().strip()

# Initial Delta. Set higher than 0 to start
max_error			= 10.0
iterations			= 1

# Print info
print "Input: ", input_file
print "Alpha: ", alpha
print "Convergence: ", convergence_error
print "Total nodes: ", node_number
if max_iterations != "":
	print "Max iterations: ", max_iterations

print 
print
# Work on temporary file
# single node
# os.popen('cp ' + input_file + ' graphstep.txt')
# Multi-node
os.popen("hadoop dfs -cp /user/hduser/" + input_file + " /user/hduser/newrank_graphstep")

while (max_error >= float(convergence_error)):
	
	print "--- Iteration:", iterations, " ---" 
	# Start measuring time
        start_time = time.time()
	
	# 1. Get dangling node sum
	# Single Node
	# dangling_sum = os.popen("cat graphstep.txt | ./newrank_dangling.py " + node_number + " | ./newrank_dangling_reducer.py").read().strip()
	# dangling_sum = float(dangling_sum)

	# Multi node 
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NewRank Dangling Iteration " + str(iterations) + "' -D mapred.reduce.tasks=1 -file newrank_dangling.py -file newrank_dangling_reducer.py -input /user/hduser/newrank_graphstep -output /user/hduser/newrank_dangling -mapper newrank_dangling.py\ " + str(node_number) + " -reducer  newrank_dangling_reducer.py")
	dangling_sum = os.popen("hadoop dfs -cat /user/hduser/newrank_dangling/part*").read().strip()
	dangling_sum = float(dangling_sum)
	os.popen("hadoop dfs -rmr /user/hduser/newrank_dangling")
	print "Dangling nodes sum:", dangling_sum

	# 2. Do basic iteration step
	# Single node
	# os.popen('cat graphstep.txt | ./newrank_map.py | sort -k1,1 | ./newrank_reduce.py ' + alpha + ' ' + node_number + ' ' + str(dangling_sum)  + ' > nextstep.txt')
	# Multi-Node
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NewRank Iteration " + str(iterations) + "' -D mapred.reduce.tasks=10 -file newrank_map.py -file newrank_reduce.py -input /user/hduser/newrank_graphstep -output /user/hduser/newrank_nextstep -mapper newrank_map.py -reducer newrank_reduce.py\ " + str(alpha) + "\ " + str(node_number) + "\ " + str(dangling_sum))

	# 3. Get iteration ERROR
	# Single node
	# max_error = os.popen('cat nextstep.txt | ./newrank_error_map.py | ./newrank_error_reduce.py').read().strip()
	# max_error = float(max_error)
	# print "Max error: ", max_error

	# Multi-Node
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='NewRank Error Calculation " + str(iterations) + "' -D mapred.reduce.tasks=1 -file newrank_error_map.py -file newrank_error_reduce.py -input /user/hduser/newrank_nextstep -output /user/hduser/newrank_error -mapper newrank_error_map.py -reducer newrank_error_reduce.py")
        max_error = os.popen("hadoop dfs -cat /user/hduser/newrank_error/part*").read().strip()
        max_error = float(max_error)
	print "Max error: ", max_error
	
	iterations += 1 
	
	# Multi-Node
        os.popen("hadoop dfs -rmr /user/hduser/newrank_error /user/hduser/newrank_graphstep")

        # Single-Node   
        # os.popen('mv nextstep.txt graphstep.txt')
        # Multi-Node
        os.popen("hadoop dfs -mv /user/hduser/newrank_nextstep /user/hduser/newrank_graphstep")
        os.popen("hadoop dfs -rmr /user/hduser/newrank_graphstep/_*")
		

	if (max_iterations != "" and iterations >= int(max_iterations)):
		break
		
	print

        # Measure and print elapsed time
        print "--- Iteration: " + str(iterations) +  ": %s seconds ---" % (time.time() - start_time)

# Single Node
# os.popen("export LC_ALL=C; cat graphstep.txt | while IFS=$'\t' read author dats score rho rho_prior; do final_score=${dats##*|}; echo \"${author}\t${dats}\t${score}\t${final_score}\t${rho}\t${rho_prior}\"; done | sort -t'\t' -k4 -gr > " + input_file + "_newrank_a" + str(alpha) + "_i" + str(iterations) + ".txt")
# os.popen('rm graphstep.txt')

os.popen("hadoop dfs -mv /user/hduser/newrank_graphstep /user/hduser/NR_" + input_file + "_a" + str(alpha) + "_error" + str(max_error) + "_i" + str(iterations) + "")
	




