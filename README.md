### Paper Ranking Method Implementations

#### Ilias Kanellos, IMIS, Athena Research Center

In this repository we provide open source implementations of paper ranking methods that have been proposed in  
the literature. Our implementations utilise a suite of map-reduce scripts and can be used either on a single machine,
or a hadoop cluster. 

All codes were developed in the context of a paper ranking survey that aimed to evaluate each method's strengths and weaknesses.

##### Running Ranking Methods

The Ranking methods provided consist of a main script and a number of complementaty map-reduce scripts.
Most methods are iterative and the complementary map-reduce scripts implement the various steps of a single
iteration. The main script given for each method orchestrates the iterations (single step calculations,
convergence error computation etc.). All main scripts are named after the method they implement.

When running the methods on a single machine, linux is required, since all scripts rely on bash commands. 
Any map-reduce procedure can be easily emulated through a bash command of the form:

	cat input_file.txt | ./map_script.py | sort -k1,1 | ./reduce_script.py
	
Commands such as the above are run by the main script on a single machine.

When running the methods on a cluster (e.g. using hadoop), the commands called by the main script may need specific 
parameter tuning. In the main scripts we provide our own version of the hadoop commands used, in comments. However, 
they should be modified accordingly by each user, depending on the setup/framework used.

Each script expects a number of parameters. Running a script without parameters, or with the wrong number of parameters
returns a message of how to correctly use the script. In order to run a method, the main script, as well as the complementary map-reduce scripts should all be locacted in the same directory. During the method execution, a number of temporary files will be written and deleted. The names of all temporary files are hardcoded. Thus, no more than one instance of a ranking script should run at any given time, in any directory, to avoid errors. 

#### Input File Formats

The input files each method reads contain citation data and paper scores.
Care has been given so that all implementations use the same input file format.
Thus, a single input file can be used to calculate ranking results for all methods.

Our methods use two types of input files: 

* Files containing paper data (citations, etc.)
* Files containing author data

#### Paper Data Input Files

These files must contain lines of the following format: 

	<paper id>\t<comma separated referenced paper id list>|<number of referenced papers>|<current/initial paper score>\t<previous paper score>\t<publication year>

In the above, parts enclosed by tags denote variables. "\t" denotes a single tab character. The remaining parts are literals.

An example line of a paper input file is the following:

	0201001 9310158,0106130,9210109|3|0.000000000001	0	2002

This line denotes that paper with id 0201001 references 3 other papers, namely 9310158,0106130,9210109. The paper was published in 2002.
It's current score is 0.000000000001 and its previous score was 0.

**NOTE:** All paper ids MUST begin with a digit.

**ADDITIONAL NOTE:** Lines for papers that don't reference other papers should have the following form:

	0|0|<current paper score>

#### Author Data Input Files

FutureRank requireσ an additional author input file. Each line in this file should have the following format:

	<author name (no spaces)>\t<list of <paper>/<number of collaborators>>|<current author score>\t<previous author score>

An example is the following:

	o.dippel	10.1103/PhysRevA.49.4415/3|2.57270463293e-06	0

This line denotes that an author named "o.dippel" has written the paper with id "10.1103/PhysRevA.49.4415" in which he was one of 3 collaborators. The author's current score is 2.57270463293e-06, and his previous score is 0.

**NOTE:** All author name must begin with a letter character.
**NOTE2:** To run futurerank in a cluster, author data files stored in a distributed directory, must begin with "part"

#### Ranking Method Output Files

Each ranking script outputs a single result file. Most of the parameters used for running the method are included in 
the resulting file's name. Papers are not sorted by score, but their final score values are included. The format 
of each output file is similar to that of the input files. 

In most cases, the final paper score can be found either in a final column, after the publication year, or in
the last part of column 2. Exceptions to this rule are CiteRank and TimeAwareRanking in ECM mode. For these methods, 
the paper scores are found in column 3.

#### Licence

All codes are provided under a gnu/gpl licence.

