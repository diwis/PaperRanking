#!/usr/bin/python

# Program to caculate FutureRank scores
# Input: file read from pipe
# Parameters: 
# 1) pagerank input file
# 2) author paper file
# 3) a value
# 4) b value
# 5) c value
# 6) p value (factor in exponential formula)
# 7) current year (setting current time)
# 8) convergence error
# 9) optional: number of iterations

# Imports
import sys
import os

# Read and initialise arguments
if(len(sys.argv) < 9):
	print "Usage: ./futurerank.py <pagerank_input_file> <author_paper_input_file> <alpha> <beta> <gamma> <exponential_factor> <current_year> <convergence_error> <optional: number_iterations>"
	sys.exit(0)
else:
	pagerank_input = sys.argv[1]
	author_paper_input = sys.argv[2]
	alpha = float(sys.argv[3])
	beta = float(sys.argv[4])
	gamma = float(sys.argv[5])
	exponential_ro = float(sys.argv[6])
	current_year = sys.argv[7]
	max_delta = float(sys.argv[8])
	try:
		num_iterations = int(sys.argv[9])
	except (IndexError, NameError):
		num_iterations = 0
		
# Set an initial (high) error value
delta = 100

# Get numbers of authors and papers
# -- Do this on hadoop
# num_of_authors = os.popen('wc -l ' + author_paper_input + ' | cut -d " " -f 1').read()
# num_of_papers = os.popen('wc -l ' + pagerank_input + ' | cut -d " " -f 1').read()
num_of_authors = os.popen("hadoop dfs -cat /user/hduser/" + author_paper_input + "/* | wc -l | cut -d ' ' -f 1").read().strip()
num_of_papers = os.popen("hadoop dfs -cat /user/hduser/" + pagerank_input + "/* | wc -l | cut -d ' ' -f 1").read().strip()

# Print initialisation messages

print
print "Number of Authors: ", num_of_authors
print "Number of Papers: ", num_of_papers
print "Alpha: ", alpha
print "Beta: ", beta
print "Gamma: ", gamma
print "Exponential ro: ", exponential_ro
print "Query Year: ", current_year
print "Paper citation file: ", pagerank_input
print "Author Rank file: ", author_paper_input
print
print

# Get the paper input file as graphstep file
# -- Move the input to hadoop dfs directories
# os.popen('cp ' + pagerank_input + ' paper_graphstep.txt')
# os.popen('cp ' + author_paper_input + ' author_graphstep.txt')
os.popen("hadoop dfs -cp /user/hduser/" + author_paper_input + " /user/hduser/futurerank_author/")
os.popen("hadoop dfs -cp /user/hduser/" + pagerank_input + " /user/hduser/futurerank_paper/")

iterations = 0
	
# Do rounds of the futureRank algorithm
while(delta >= max_delta):
	
	if(iterations >= num_iterations and num_iterations != 0):
		break;
	
	iterations += 1
	print "Iteration #" + str(iterations)

	#1. The authors state that "Furthermore, for any paper pi which does not cite any 
	#   other article in the dataset, we define Mi,j =  1, for all j.  This means that
	#   we have to run a mapreduce job to get the sum of ranking scores for dangling nodes.
	#   Hassan Sayyadi stated that there should actually be random walk probabilities, so we get the sum of probabilities
	#dangling_sum = os.popen("cat paper_graphstep.txt | ./futurerank_dangling_map.py " + str(num_of_papers) + " | sort -k1,1 | ./futurerank_dangling_reducer.py").read()

	# -- Get dangling sum through hadoop job
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='FutureRank Dangling Iteration " + str(iterations) + "' -D mapred.reduce.tasks=1 -file futurerank_dangling_map.py -file futurerank_dangling_reducer.py -input /user/hduser/futurerank_paper/ -output /user/hduser/futurerank_paper_dangling -mapper futurerank_dangling_map.py\ " + str(num_of_papers) + " -reducer futurerank_dangling_reducer.py")
	dangling_sum = os.popen("hadoop dfs -cat /user/hduser/futurerank_paper_dangling/part*").read().strip()
	# Remove directory 
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_paper_dangling")
	# dangling_sum = str(dangling_sum)
	# dangling_sum = dangling_sum.strip()
	
	print "\nDangling Sum: ", dangling_sum

	#2. Do the futureRank paper score calculation in a mapreduce fashion
	#   For author files there should be info on how many authors collaborate on each paper
	#os.popen("cat paper_graphstep.txt author_graphstep.txt | ./futurerank_paper_map.py " + str(exponential_ro) + " " + str(current_year) + " | sort -k1,1 | ./futurerank_paper_reduce.py " + dangling_sum + " " + str(num_of_papers) + " " + str(num_of_authors) + " | sort -k1,1 > paper_nextstep.txt")
	
	# -- Do this on hadoop
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='FutureRank Paper Iteration " + str(iterations) + "' -D mapred.reduce.tasks=10 -file futurerank_paper_map.py -file futurerank_paper_reduce.py -input /user/hduser/futurerank_* -output /user/hduser/futurerank_paper_nextstep -mapper futurerank_paper_map.py\ " + str(exponential_ro) + "\ " + str(current_year) + " -reducer futurerank_paper_reduce.py\ " + str(dangling_sum) + "\ " + str(num_of_papers) + "\ " + str(num_of_authors) + "")
	
	#3. Get sums of each coefficient in order to normalize them...
	# -- Do this on hadoop
	# sum_parts = os.popen("cat paper_nextstep.txt | ./sum_coefficients_map.py | sort | ./sum_coefficients_reduce.py").read()
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='FutureRank Paper Normalisation Coefficients " + str(iterations) + "' -D mapred.reduce.tasks=1 -file sum_coefficients_map.py -file sum_coefficients_reduce.py -input /user/hduser/futurerank_paper_nextstep -output /user/hduser/futurerank_paper_coefficients -mapper sum_coefficients_map.py -reducer sum_coefficients_reduce.py")
	sum_parts = os.popen("hadoop dfs -cat /user/hduser/futurerank_paper_coefficients/part*").read().strip()
	
	print "Sum parts: ", sum_parts
	sum_parts = str(sum_parts).replace("\t", "\ ")
	
	#4. Calculate normalized scores for papers, based on futurerank formula
	# os.popen("cat paper_nextstep.txt | sort -k1,1 | ./futurerank_paper_normalize_reduce.py " + sum_parts + " " + str(alpha) + " " + str(beta) + " " + str(gamma) + " " + str(num_of_papers) + " > paper_normalized.txt")
	# -- Do this on hadoop
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='FutureRank Paper Normalisation " + str(iterations) + "' -D mapred.reduce.tasks=10 -file futurerank_paper_normalize_reduce.py -input /user/hduser/futurerank_paper_nextstep/part* -output /user/hduser/futurerank_paper_normalized/ -mapper cat -reducer futurerank_paper_normalize_reduce.py\ " + str(sum_parts) + "\ " + str(alpha) + "\ " + str(beta) + "\ " + str(gamma) + "\ " + str(num_of_papers) + "")

	# Up to this part we have calculated normalized scores for each part of our ranking coefficients.
	# Now to calculate the rankings of authors!

	#5. Do the futureRank author score calculation in a mapreduce fashion
	# -- Do it on - wait for it... HADOOP.
	# os.popen("export LC_ALL=UTF-8; cat author_graphstep.txt paper_normalized.txt | ./futurerank_author_map_one.py | sort -k1,1 | ./futurerank_author_reduce_one.py | sort -k1,1 | ./futurerank_author_reduce_two.py > author_nextstep.txt")
	# What we did in one pipe job on a single computer needs now two mapreduce jobs	
	file_list = os.popen("hadoop dfs -ls /user/hduser/futurerank_author/part* | grep -o 'part.*'").read().strip().split()
	for dfs_file in file_list:
		os.popen("hadoop dfs -mv /user/hduser/futurerank_author/" + dfs_file + " /user/hduser/futurerank_author/" + dfs_file.replace("part", "author_part") + "")
	os.popen("hadoop dfs -cp /user/hduser/futurerank_author/author_part* /user/hduser/futurerank_paper_normalized/")
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_paper_normalized/_*")
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Futurerank Author Calculation A:" + str(iterations) + "' -D mapred.reduce.tasks=10 -file futurerank_author_map_one.py -file futurerank_author_reduce_one.py -input /user/hduser/futurerank_paper_normalized/ -output /user/hduser/futurerank_author_nextstepA/ -mapper futurerank_author_map_one.py -reducer futurerank_author_reduce_one.py")
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Futurerank Author Calculation B:" + str(iterations) + "' -D mapred.reduce.tasks=10 -file futurerank_author_reduce_two.py -input /user/hduser/futurerank_author_nextstepA/ -output /user/hduser/futurerank_author_nextstepB/ -mapper cat -reducer futurerank_author_reduce_two.py")

	# Remove logs
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_author_nextstepB/_*")

	# sys.exit(0)
	#6. Normalize author scores!
	# -- What a surprise again ! HADOOP IT
	#author_score_sum = os.popen("cat author_nextstep.txt | ./futurerank_sum_score_map.py | sort -k1,1 | ./futurerank_sum_score_reduce.py").read()
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Futurerank Author Coefficients:" + str(iterations) + "' -D mapred.reduce.tasks=1 -file futurerank_sum_score_map.py -file futurerank_sum_score_reduce.py -input /user/hduser/futurerank_author_nextstepB/ -output /user/hduser/futurerank_author_coefficients/ -mapper futurerank_sum_score_map.py -reducer futurerank_sum_score_reduce.py")
	author_score_sum = os.popen("hadoop dfs -cat /user/hduser/futurerank_author_coefficients/part*").read().strip()
	print "Author score sum:", author_score_sum
	# Normalize scores on hadoop
	#os.popen("cat author_nextstep.txt | ./futurerank_normalize_map.py " + author_score_sum + " > author_normalized.txt")
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Futurerank Author Normalization " + str(iterations) + "' -D mapred.reduce.tasks=0 -file futurerank_normalize_map.py -input /user/hduser/futurerank_author_nextstepB/ -output /user/hduser/futurerank_author_normalized/ -mapper futurerank_normalize_map.py\ " + str(author_score_sum) + "")

	os.popen("hadoop dfs -rmr /user/hduser/futurerank_paper_normalized/author_part*")
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_author_normalized/_*")
		
	#7. Calculate the errors -
	# -- On hadoop
	# error = os.popen("cat paper_normalized.txt author_normalized.txt | ./futurerank_error_map.py |  sort -k1,1 | ./futurerank_error_reduce.py").read()
	os.popen("$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/*streaming*.jar -D mapred.job.name='Futurerank Error Calculation " + str(iterations) + "' -D mapred.reduce.tasks=1 -file futurerank_error_map.py -file futurerank_error_reduce.py -input /user/hduser/futurerank_*_normalized/ -output /user/hduser/futurerank_error -mapper futurerank_error_map.py -reducer futurerank_error_reduce.py")
	error = os.popen("hadoop dfs -cat /user/hduser/futurerank_error/part*").read().strip()
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_error")
	print "Error: ", error
	
	delta = float(error)

	#8. Move results to new file
	# -- ON hadoop!
	#os.popen("mv paper_normalized.txt paper_graphstep.txt")
	#os.popen("mv author_normalized.txt author_graphstep.txt")
	# os.popen("rm paper_nextstep.txt author_nextstep.txt")
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_paper/* /user/hduser/futurerank_author/*")
	os.popen("hadoop dfs -mv /user/hduser/futurerank_paper_normalized/part* /user/hduser/futurerank_paper/")
	os.popen("hadoop dfs -mv /user/hduser/futurerank_author_normalized/part* /user/hduser/futurerank_author/")
	os.popen("hadoop dfs -rmr /user/hduser/futurerank_author_nextstepA /user/hduser/futurerank_author_nextstepB /user/hduser/futurerank_paper_nextstep /user/hduser/futurerank_paper_coefficients /user/hduser/futurerank_author_coefficients /user/hduser/futurerank_author_normalized /user/hduser/futurerank_paper_normalized") 


	#break



# Don't forget to output in new file - keep stats in there
os.popen("hadoop dfs -mv /user/hduser/futurerank_paper /user/hduser/FR_paper_a" + str(alpha) + "_b" + str(beta) + "_c" + str(gamma) + "_r" + str(exponential_ro) + "_y" + str(current_year) + "_error" + str(max_delta) + "_i" + str(iterations) + "")
os.popen("hadoop dfs -mv /user/hduser/futurerank_author /user/hduser/FR_author_a" + str(alpha) + "_b" + str(beta) + "_c" + str(gamma) + "_r" + str(exponential_ro) + "_y" + str(current_year) + "_error" + str(max_delta) + "_i" + str(iterations) + "")


