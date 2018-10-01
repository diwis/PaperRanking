#    yetrank_hadoop.py - YetRank 
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

# -------------------------------------------------------------------- #

import sys
import os
import time

# -------------------------------------------------------------------- #

if len(sys.argv) < 4:
	print "Usage:"
	print "./yetrank_hadoop.py <input_file> <alpha> <convergence_error> <optional: max_iterations>"
	sys.exit(0)


input_file			= sys.argv[1]
alpha 				= sys.argv[2]
convergence_error	= sys.argv[3]

try:
	max_iterations 	= sys.argv[4]
except:
	max_iterations	= -1
	
# Single node 	
# num_nodes = os.popen('wc -l ' + input_file + ' | cut -f 1 -d " "').read().strip()
# Multi-node
num_nodes = os.popen("hadoop dfs -cat /user/hduser/" + input_file + "/* | wc -l | cut -d ' ' -f 1").read().strip()
error	  = 100


print "Input:", input_file
print "Alpha:", alpha
print "Convergence: ", convergence_error
print "Nodes:", num_nodes

# Single node command
# os.popen('cp ' + input_file + ' graphstep.txt')
# Multi-node command
os.popen("hadoop dfs -cp /user/hduser/" + input_file + " /user/hduser/yetrank_graphstep")

# ------ Iterate -------------- #
iterations = 1

while float(error) > float(convergence_error):

        # Start measuring time
        start_time = time.time()
	
	print "----- Iteration:", iterations, '-----'
	
	# 1. Calculate dangling node sum
	# Single node
	# dangling_node_sum = os.popen('cat graphstep.txt | ./yetrank_dangling.py ' + num_nodes + ' | ./yetrank_dangling_reducer.py').read().strip()
	# Multi-node
	# -- Get dangling sum through hadoop job
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='YetRank Dangling Iteration " + str(iterations) + "' -D mapred.reduce.tasks=1 -file yetrank_dangling.py -file yetrank_dangling_reducer.py -input /user/hduser/yetrank_graphstep -output /user/hduser/yetrank_paper_dangling -mapper yetrank_dangling.py\ " + str(num_nodes) + " -reducer yetrank_dangling_reducer.py")
	dangling_sum = os.popen("hadoop dfs -cat /user/hduser/yetrank_paper_dangling/part*").read().strip()
	# Remove directory 
	os.popen("hadoop dfs -rmr /user/hduser/yetrank_paper_dangling") 
	print "Dangling node sum:", dangling_sum
	
	# 2. Do basic step-calculation
	# print 'cat graphstep.txt | ./yetrank_map.py | sort -k1,1 | ./yetrank_reduce.py ' + alpha + ' ' + dangling_node_sum + ' > nextstep.txt'
	# Single Node
	# os.popen('cat graphstep.txt | ./yetrank_map.py | sort -k1,1 | ./yetrank_reduce.py ' + alpha + ' ' + dangling_node_sum + ' > nextstep.txt')
	# Multi-Node
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='YetRank Iteration " + str(iterations) + "' -D mapred.reduce.tasks=10 -file yetrank_map.py -file yetrank_reduce.py -input /user/hduser/yetrank_graphstep -output /user/hduser/yetrank_nextstep -mapper yetrank_map.py -reducer yetrank_reduce.py\ " + str(alpha) + "\ " + str(dangling_sum))
	
	
	# 3. Calculate Error
	# Single node
	# error = os.popen('cat nextstep.txt | ./yetrank_error_map.py | ./yetrank_error_reduce.py').read().strip()
	# Multi-Node
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='YetRank Error Calculation " + str(iterations) + "' -D mapred.reduce.tasks=1 -file yetrank_error_map.py -file yetrank_error_reduce.py -input /user/hduser/yetrank_nextstep -output /user/hduser/yetrank_error -mapper yetrank_error_map.py -reducer yetrank_error_reduce.py")
	error = os.popen("hadoop dfs -cat /user/hduser/yetrank_error/part*").read().strip()
	error = float(error)
	print "Max Error:", error
	# Remove error output fil
	os.popen("hadoop dfs -rmr /user/hduser/yetrank_error /user/hduser/yetrank_graphstep")
	
	# Single-Node	
	# os.popen('mv nextstep.txt graphstep.txt')
	# Multi-Node
	os.popen("hadoop dfs -mv /user/hduser/yetrank_nextstep /user/hduser/yetrank_graphstep")
	os.popen("hadoop dfs -rmr /user/hduser/yetrank_graphstep/_*")
	
	if ((int(max_iterations) > 0) and (iterations > int(max_iterations))):
		break
		
	iterations += 1

        # Measure and print elapsed time
        print "--- Iteration: " + str(iterations) +  ": %s seconds ---" % (time.time() - start_time)


		
# Single-Node
# os.popen("export LC_ALL=C; cat graphstep.txt | while IFS=$'\t' read author dats score year weight; do final_score=${dats##*|}; echo \"${author}\t${dats}\t${score}\t${year}\t${weight}\t${final_score}\"; done | sort -t'\t' -k6 -gr > " + input_file + "_yetrank_a" + str(alpha) + "_i" + str(iterations) + ".txt")
# os.popen('rm graphstep.txt')

os.popen("hadoop dfs -mv /user/hduser/yetrank_graphstep /user/hduser/YR_" + input_file + "_a" + str(alpha) + "_error" + str(error) + "_i" + str(iterations) + "")


	
	
	
	
