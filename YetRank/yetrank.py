#    yetrank.py - YetRank 
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

# -------------------------------------------------------------------- #

if len(sys.argv) < 4:
	print "Usage:"
	print "./yetrank.py <input_file> <alpha> <convergence_error> <optional: max_iterations>"
	sys.exit(0)


input_file			= sys.argv[1]
alpha 				= sys.argv[2]
convergence_error	= sys.argv[3]

try:
	max_iterations 	= sys.argv[4]
except:
	max_iterations	= -1
	
	
num_nodes = os.popen('wc -l ' + input_file + ' | cut -f 1 -d " "').read().strip()
error	  = 100


print "Input:", input_file
print "Alpha:", alpha
print "Convergence: ", convergence_error
print "Nodes:", num_nodes

os.popen('cp ' + input_file + ' graphstep.txt')

# ------ Iterate -------------- #
iterations = 1

while float(error) > float(convergence_error):
	
	print "----- Iteration:", iterations, '-----'
	
	# 1. Calculate dangling node sum
	dangling_node_sum = os.popen('cat graphstep.txt | ./yetrank_dangling.py ' + num_nodes + ' | ./yetrank_dangling_reducer.py').read().strip()
	print "Dangling node sum:", dangling_node_sum
	
	# 2. Do basic step-calculation
	# print 'cat graphstep.txt | ./yetrank_map.py | sort -k1,1 | ./yetrank_reduce.py ' + alpha + ' ' + dangling_node_sum + ' > nextstep.txt'
	os.popen('cat graphstep.txt | ./yetrank_map.py | sort -k1,1 | ./yetrank_reduce.py ' + alpha + ' ' + dangling_node_sum + ' > nextstep.txt')
	
	# 3. Calculate Error
	error = os.popen('cat nextstep.txt | ./yetrank_error_map.py | ./yetrank_error_reduce.py').read().strip()
	print "Max Error:", error
	
	os.popen('mv nextstep.txt graphstep.txt')
	
	if ((int(max_iterations) > 0) and (iterations > int(max_iterations))):
		break
		
	iterations += 1
		

os.popen("export LC_ALL=C; cat graphstep.txt | while IFS=$'\t' read author dats score year weight; do final_score=${dats##*|}; echo \"${author}\t${dats}\t${score}\t${year}\t${weight}\t${final_score}\"; done | sort -t'\t' -k6 -gr > " + input_file + "_yetrank_a" + str(alpha) + "_i" + str(iterations) + ".txt")
os.popen('rm graphstep.txt')

	
	
	
	
