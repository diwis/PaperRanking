# PaperRanking

# Ilias Kanellos, IMIS, Athena Research Center
# This is a collection of Paper Ranking method implementations, developed for a survey we wrote.

# -------- How to run ranking method codes -------- #

All Ranking methods provided consist of a main script and a number of complementaty map-reduce scripts.
Most methods are iterative and the complementary map-reduce scripts implement various steps of a single
iteration. The main script given is the one that orchestrates the iterations (single step calculations,
convergence error computation etc). The main scripts are named after the method they implement.

The main script is responsible for running the appropriate map-reduce commands in each step. Methods 
can not only be run on hadoop (or another map-reduce framework), but also on a single machine. In the 
latter case, linux is required, since all scripts relie on bash commands. Any map-reduce procedure can
be easily emulated through a bash command of the form:

	cat input_file.txt | ./map_script.py | sort -k1,1 | ./reduce_script.py
	
Therefore, we provide commands as the above in every script. 

Hadoop commands on the other hand may need specific tuning of parameters. We provide our own version 
used for them in comments, but they should be modified accordingly.

Each script expects a number of parameters. If we run a main script without parameters, we will get a 
message of correct usage. All scripts for each method should be in the same directory. A number of files
will be written/deleted during the calculations conducted by each script. Since these files have specific
names, no more than one instance of a ranking script should run at any given time, in any directory. If
more than one script is running the scripts will crash.

# ----------------------------------------------------- #

# -------- Expected input -------- #

All ranking scripts expect an input file name, among a number of other parameters.
Care has been given so that the input file format is the same for all methods, and
thus, a single input file can be used to calculate ranking results for all methods.

# --- PAPER INPUT --- #
An input file that contains paper data should be comprised of lines in the following
form: 

	<paper id>\t<comma separated referenced paper id list>|<number of referenced papers>|<current/initial paper score>\t<previous paper score>\t<publication year>

In the above, we use "<>" to denote variable parts. Anything not enclosed in tags is a literal.
"\t" denotes a single tab character.

An example would be:

	0201001 9310158,0106130,9210109|3|0.000000000001	0	2002

This line denotes that paper 0201001 references 3 other papers, namely 9310158,0106130,9210109 and was published in 2002.
It's current score is 0.000000000001 and its previous score was 0.

NOTE: All paper ids MUST begin with a digit.

ADDITIONAL NOTE: papers that reference no other papers should have the following form for their second column:

	0|0|<current paper score>

# --- AUTHOR INPUT --- #

Scripts such as FutureRank require an additional author input file. This should have the following form:

	<author name (no spaces)>\t<list of <paper>/<number of collaborators>>|<current author score>\t<previous author score>

An example is:

	o.dippel	10.1103/PhysRevA.49.4415/3|2.57270463293e-06	0

This denotes that an author named "o.dippel" has written the paper with id "10.1103/PhysRevA.49.4415" in which he was one of 3 collaborators. The author's current score is 2.57270463293e-06, and his previous score is 0.

NOTE: ALL author name MUST begin with a letter character.

# ------------------------------------------------------- #

# ------------- Output ---------------------------------- #

Each ranking script will after completion output a final file. This file will include most of the parameters
used for running the method. This file is not necessarily sorted based on paper scroes, but will include their
final values. Its format is similar to that of the input. 

In most cases, the final paper score can be found either in a final column, after the publication year, or in
the last part of column 2. Exceptions to this are CiteRank and TimeAwareRanking. For these methods, the paper
scores should be found in column 3, except when running TimeAwareRanking in RAM mode, where column 2 is valid.

# -------------------------------------------------------- #

All codes are provided under a gnu/gpl licence.

