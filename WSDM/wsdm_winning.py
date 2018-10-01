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

# Imports
import os
import sys
import math

# -------------------------------------------------------------------- #

# -------------------------------------------------------------------- #
# Function Definitions

def get_node_degrees(paper_graph_file):
	in_degree_dict = dict()
	out_degree_dict = dict()
	
	paper_citation_dict = dict()
	
	with open(paper_graph_file) as f:
		for line in f:
			line = line.strip()
			# Get data parts
			paper, data, prev_score, year = line.split()
			# Get outlinks
			cited_papers, cited_num, score = data.split("|")
			
			if paper not in paper_citation_dict:
				paper_citation_dict[paper] = []
			
			# Add citing paper's outdegree
			out_degree_dict[paper] = int(cited_num)
			if paper not in in_degree_dict:
				in_degree_dict[paper] = 0
			
			# Loop cited papers
			if cited_num != "0":
				cited_list = cited_papers.split(",")
				for cited_paper in cited_list:
					if cited_paper not in out_degree_dict:
						out_degree_dict[cited_paper] = 0
					if cited_paper not in in_degree_dict:
						in_degree_dict[cited_paper] = 0
					in_degree_dict[cited_paper] += 1
					
					# Add cited papers
					paper_citation_dict[paper].append(cited_paper)
	
	return in_degree_dict, out_degree_dict, paper_citation_dict
	
########################################################################

def initialise_paper_scores(in_degrees, out_degrees, alpha, beta):
	initial_score_dict = dict()
	max_in_degree  = max(in_degrees.values())
	max_out_degree = max(out_degrees.values())
	
	for paper in in_degrees:
		first_addend  = alpha * (float(in_degrees[paper])/float(max_in_degree))
		second_addend = math.pow(float(out_degrees[paper])/float(max_out_degree), beta)
		initial_score_dict[paper] = float(first_addend + second_addend) / float(1+alpha)
		# print >>sys.stderr, "Addends: ", first_addend, "-", second_addend, "| Score:",  float(first_addend + second_addend) / float(1-alpha)
		
	return initial_score_dict
	
########################################################################

def get_paper_venue_dict(venue_file, paper_initial_score_dict):
	paper_venue_dict = dict()
	with open(venue_file) as f:
		for line in f:
			line = line.strip()
			paper, venue = line.split("\t")
			if paper in paper_initial_score_dict:
				paper_venue_dict[paper] = venue
			
			'''
			if venue not in paper_venue_file:
				paper_venue_dict[venue] = list()
			paper_venue_dict[venue].append(paper)
			'''
				
	return paper_venue_dict

########################################################################

########################################################################

def get_author_paper_dict(author_file, paper_score_dict):
	author_paper_dict = dict()
	with open(author_file) as f:
		for line in f:
			line = line.strip()
			author, paper_data, score = line.split()
			
			papers, prev_scores = paper_data.split("|")
			
			papers_list = papers.split(",")
			actual_papers_list = [paper_parts.split("/")[0] for paper_parts in papers_list if paper_parts.split("/")[0] in paper_score_dict]
			
			if author not in author_paper_dict:
				author_paper_dict[author] = list()
			for actual_paper in actual_papers_list:
				author_paper_dict[author].append(actual_paper)
			
	return author_paper_dict

########################################################################

def propagate_paper_score_to_venue(paper_score_dict, venue_dict):
	# Create a dictionary for venues/authors/papers
	venue_score_dict = dict()
	
	num_papers_per_venue_dict = dict()
	# Loop papers and add their score to the venue
	for paper in paper_score_dict:
		paper_score = paper_score_dict[paper]
		# Get other venue
		venue = venue_dict[paper]
		
		# Count paper in the venue dictionary
		if venue not in num_papers_per_venue_dict:
			num_papers_per_venue_dict[venue] = 0
		num_papers_per_venue_dict[venue] += 1
		
		# Add other score
		if venue not in venue_score_dict:
			venue_score_dict[venue] = 0
		venue_score_dict[venue] += paper_score
		
	# Done with loop, now to make it avg.
	for venue in num_papers_per_venue_dict:
		venue_score_dict[venue] = float(venue_score_dict[venue])/float(num_papers_per_venue_dict[venue])
		
	print >>sys.stderr, "Found ", len(num_papers_per_venue_dict), "|", len(venue_score_dict), "venues"
	
	return venue_score_dict
	
	
########################################################################

def propagate_paper_score_to_author(paper_score_dict, author_paper_dict):
	# Create a dictionary for venues/authors/papers
	author_score_dict = dict()
	
	# Loop authors and add their score to the venue
	for author in author_paper_dict:
		# print
		# print "Examining author: ", author
		paper_counter = 0
		for paper in author_paper_dict[author]:
			# print "Examining paper: ", paper
			# Check if that paper is in the valid papers examined - otherwise discard it.
			# Do this because the author file could be containing ALL data
			if paper in paper_score_dict:
				paper_score = paper_score_dict[paper]
				paper_counter += 1
				# print "Incremented paper counter!"
				if author not in author_score_dict:
					author_score_dict[author] = 0				
				author_score_dict[author] += paper_score
		
		# print "Paper counter = ", paper_counter
		if author in author_score_dict:		
			author_score_dict[author] = float(author_score_dict[author])/float(paper_counter)
		
	print >>sys.stderr, "Found ", len(author_score_dict), "|", len(author_paper_dict), "authors"
	
	return author_score_dict
	
	
########################################################################

########################################################################

def propagate_paper_score_to_paper(paper_score_dict, paper_citation_dict):
	
	# Create a dictionary for venues/authors/papers
	new_paper_score_dict = dict()
	num_citing_papers = dict()
	
	# Loop authors and add their score to the venue
	for paper in paper_citation_dict:
		# print
		# print "Examining paper: ", paper
		if paper not in new_paper_score_dict:
			new_paper_score_dict[paper] = 0
		if paper not in num_citing_papers:
			num_citing_papers[paper] = 0
		for cited_paper in paper_citation_dict[paper]:
			# print "Examining cited paper: ", cited_paper
			
			if cited_paper not in new_paper_score_dict:
			 	new_paper_score_dict[cited_paper] = 0
			
			if cited_paper not in num_citing_papers:
				num_citing_papers[cited_paper] = 0
				
			new_paper_score_dict[cited_paper] += paper_score_dict[paper]
			num_citing_papers[cited_paper] += 1
			
	for paper in new_paper_score_dict:
		if num_citing_papers[paper] != 0:
			# print >>sys.stderr, paper, "cited by", num_citing_papers[paper], "score: ", new_paper_score_dict[paper], "/", num_citing_papers[paper], "=", str(float(new_paper_score_dict[paper])/float(num_citing_papers[paper]))
			new_paper_score_dict[paper] = float(new_paper_score_dict[paper])/float(num_citing_papers[paper])
		
	print >>sys.stderr, "Found ", len(new_paper_score_dict), "|", len(paper_citation_dict), "|", len(paper_score_dict), "papers"
	
	return new_paper_score_dict
	
########################################################################

# First dict has paper - venue match. Second has paper - author match. 
# Third has valid authors that should be checked. Fourth has valid papers.
def get_author_venue_dict(paper_venue_dict, paper_author_dict, author_score_dict, paper_score_dict):
	author_venue_dict = dict()
	
	# Get for each author all his papers. 
	# Do this for the valid authors
	for author in author_score_dict:	
		author_venue_dict[author] = set()
		# For each of his papers get the venue
		for authored_paper in paper_author_dict[author]:
			if authored_paper in paper_score_dict:
				# If venue not in dictionary/set, add it
				paper_venue = paper_venue_dict[authored_paper]
				author_venue_dict[author].add(paper_venue)
	
	return author_venue_dict
	
	
########################################################################

# Get the new author score as avg between score from papers and score from venues
def refine_author_scores(author_score_dict, author_venue_dict, venue_score_dict):
	
	for author in author_score_dict:
		author_current_score = author_score_dict[author]
		author_aggregate_venue_score = sum([venue_score_dict[venue] for venue in author_venue_dict[author]])
		author_averaged_venue_score = float(author_aggregate_venue_score) / float(len(author_venue_dict[author]))
		
		# Refined author score is avg between the two
		author_new_score = (author_current_score + author_averaged_venue_score) / float(2)
		author_score_dict[author] = author_new_score
		# print >>sys.stderr, author, " new score: ", author_new_score
	
	return author_score_dict
	
########################################################################

def get_score_through_voting_strategy(paper_initial_score_dict, author_score_dict, venue_score_dict, paper_score_dict, inverse_paper_author_dict, paper_venue_dict):
	
	new_paper_score_dict = dict()
	
	# Loop papers in initial score dict - those are also the valid ones
	for paper in paper_initial_score_dict:
		# Get paper initial score
		paper_initial_score = paper_initial_score_dict[paper]
		# Get paper venue score
		paper_venue = paper_venue_dict[paper]
		paper_venue_score = venue_score_dict[paper_venue]
		# Get paper author score avg
		paper_author_score_list = [author_score_dict[author] for author in inverse_paper_author_dict[paper]]
		avg_paper_author_score = float(sum(paper_author_score_list))/float(len(paper_author_score_list))
		# Get paper citation score
		paper_citation_score = paper_score_dict[paper]
		
		# Do the grouping strategy. P-initial is used as a separator score
		positive_group = list()
		negative_group  = list()
		
		if paper_venue_score > paper_initial_score:
			positive_group.append(paper_venue_score)
		else:
			negative_group.append(paper_venue_score)
			
		if avg_paper_author_score > paper_initial_score:
			positive_group.append(avg_paper_author_score)
		else:
			negative_group.append(avg_paper_author_score)
		
		if paper_citation_score > paper_initial_score:
			positive_group.append(paper_citation_score)
		else:
			negative_group.append(paper_citation_score)
			
		if len(positive_group) > len(negative_group):
			dominant_group = positive_group
		else:
			dominant_group = negative_group
			
		avg_dominant_group = float(sum(dominant_group))/float(len(dominant_group))
		new_paper_score_dict[paper] = float(paper_initial_score + avg_dominant_group)/float(2)

	return new_paper_score_dict
		
	
########################################################################

def get_inverse_paper_author_dict(paper_author_dict, paper_initial_score_dict):
	
	inverse_paper_author_dict = dict()
	for author in paper_author_dict:
		for paper in paper_author_dict[author]:
			if paper in paper_initial_score_dict:
				if paper not in inverse_paper_author_dict:
					inverse_paper_author_dict[paper] = set()
				inverse_paper_author_dict[paper].add(author)
			
	return inverse_paper_author_dict


########################################################################

########################################################################

def remove_disconnected_nodes(in_degree_dict, out_degree_dict):
	
	disconnected_nodes_set = set()
	for paper in in_degree_dict:
		if in_degree_dict[paper] == 0 and out_degree_dict[paper] == 0:
			# print "Paper: ", paper, "is disconnected"
			disconnected_nodes_set.add(paper)

	for paper in disconnected_nodes_set:
		in_degree_dict.pop(paper)
		out_degree_dict.pop(paper)
			
	print "Disconnected Papers: ", len(disconnected_nodes_set)
	return disconnected_nodes_set, in_degree_dict, out_degree_dict
	


########################################################################
	
# -------------------------------------------------------------------- #	
	
# -------------------------------------------------------------------- #
	
# Init & Command Line			
graph_paper_file = sys.argv[1]		
alpha			 = float(sys.argv[2])
beta 			 = float(sys.argv[3])
paper_venue_file = sys.argv[4]
author_paper_file = sys.argv[5]

# INITIALISATIONS & VARS

# Dictionary for initial scores
paper_initial_score_dict = dict()
# Dictionary for venue scores
venue_score_dict = dict()
# Dictionary for author scores
author_score_dict = dict()
# Dictionary for paper scores
paper_score_dict = dict()
# Dictionary for author - to venue matching
author_venue_dict = dict()
# inverse author/paper dict
inverse_paper_author_dict = dict()


# -------------------------------------------------------------------- #

# Get in and out degrees
in_degree_dict, out_degree_dict, paper_citation_dict = get_node_degrees(graph_paper_file)
# Remove disconnected nodes
disconnected_nodes_dict, in_degree_dict, out_degree_dict = remove_disconnected_nodes(in_degree_dict, out_degree_dict)
# Initialise based on given formula
paper_initial_score_dict = initialise_paper_scores(in_degree_dict, out_degree_dict, alpha, beta)


# Get paper - venue match
paper_venue_dict = get_paper_venue_dict(paper_venue_file, paper_initial_score_dict)
# Get author - papers match
paper_author_dict = get_author_paper_dict(author_paper_file, paper_initial_score_dict)

max_in_degree = max(in_degree_dict.values())
max_out_degree = max(out_degree_dict.values())

print "In degree dict length: ", len(in_degree_dict)
print "Out degree dict length: ", len(out_degree_dict)
print "Max in degree: ", max_in_degree
print "Max out degree: ", max_out_degree
print "Initial author dict length:", len(paper_author_dict)

print "Paper initial scores: ", len(paper_initial_score_dict)

print "Initial highest scores"
counter = 0
for w in sorted(paper_initial_score_dict, key=paper_initial_score_dict.get, reverse=True):
  print w, paper_initial_score_dict[w]
  counter += 1
  if counter > 10:
	  break
	  
print

# NOTE/TODO: Include a step that removes zero-cited papers from the graph and assign them 
# a random score at the end of all computations. 

# Iterations.

# #################################################################### #


for i in range(1, 6):
	
	print "Doing iteration: ", i
	
	# Step 1: Propagate paper score to papers, authors, and venues
	venue_score_dict  = propagate_paper_score_to_venue(paper_initial_score_dict, paper_venue_dict)
	print >>sys.stderr, "Returning venue scores. #Venues:", len(venue_score_dict)
	print
	print "Iteration ", i, "highest venue scores:"
	counter = 0
	for w in sorted(venue_score_dict, key=venue_score_dict.get, reverse=True):
	  print w, venue_score_dict[w]
	  counter += 1
	  if counter > 10:
		  break
		  
	print 
	  
	author_score_dict = propagate_paper_score_to_author(paper_initial_score_dict, paper_author_dict)
	print >>sys.stderr, "Returning author scores. #Authors:", len(author_score_dict)	
	print
	print "Iteration ", i, "highest author scores:"
	counter = 0
	for w in sorted(author_score_dict, key=author_score_dict.get, reverse=True):
	  print w, author_score_dict[w]
	  counter += 1
	  if counter > 10:
		  break
		  
	print 
	print
	paper_score_dict  = propagate_paper_score_to_paper(paper_initial_score_dict, paper_citation_dict)
	print "Iteration ", i, "highest paper scores:"
	counter = 0
	for w in sorted(paper_score_dict, key=paper_score_dict.get, reverse=True):
	  print w, paper_score_dict[w]
	  counter += 1
	  if counter > 10:
		  break

	# #################################################################### #

	print
	print

	# Step 2: Author score refinement
	# Refined author score will be the avg between the author score 
	# previously calculated, and that obtained by averaging the scores 
	# of of the venues he is published in.

	# Create this dictionary only if it's not populated yet
	if not author_venue_dict:
		author_venue_dict = get_author_venue_dict(paper_venue_dict, paper_author_dict, author_score_dict, paper_score_dict)
		
	# print dict(author_venue_dict.items()[0:50])

	# Refine author score
	author_score_dict = refine_author_scores(author_score_dict, author_venue_dict, venue_score_dict)
	print >>sys.stderr, "Returning author scores. #Authors:", len(author_score_dict)	
	print
	print "Iteration ", i, "refined highest author scores:"
	counter = 0
	for w in sorted(author_score_dict, key=author_score_dict.get, reverse=True):
	  print w, author_score_dict[w]
	  counter += 1
	  if counter > 10:
		  break
		  
	print
	print

	# Step 3: Voting Strategy
	# Get inverse paper - author dict
	if not inverse_paper_author_dict:
		inverse_paper_author_dict = get_inverse_paper_author_dict(paper_author_dict, paper_initial_score_dict)
		
	new_paper_score_dict = get_score_through_voting_strategy(paper_initial_score_dict, author_score_dict, venue_score_dict, paper_score_dict, inverse_paper_author_dict, paper_venue_dict)
	print >>sys.stderr, "Iteration ", i, "voted paper scores. #Papers:", len(new_paper_score_dict)	
	print
	counter = 0
	for w in sorted(new_paper_score_dict, key=new_paper_score_dict.get, reverse=True):
	  print w, new_paper_score_dict[w]
	  counter += 1
	  if counter > 10:
		  break
		  
	print "Resetting initial score dict and emptying author/paper/venue scores"
	# Set new initial score	  
	paper_initial_score_dict = new_paper_score_dict
	# Reset propagated scores
	venue_score_dict = dict()
	author_score_dict = dict()
	paper_score_dict = dict()
	
	if i >= 4:
		output_file = open("WSDM_" + str(i) + "iterations_" + graph_paper_file, "w")
		for w in sorted(new_paper_score_dict, key=new_paper_score_dict.get, reverse=True):
			output_file.write(w + "\t" + str(new_paper_score_dict[w]) + "\n")
		# print lowest score
		print "lowest score: ", new_paper_score_dict[w]
		# Fix score for disconnected papers
		disconnected_paper_score = float(new_paper_score_dict[w])/float(2)
		print "Disconnected node score: ", disconnected_paper_score
		for paper in disconnected_nodes_dict:
			output_file.write(paper + "\t" + str(disconnected_paper_score) + "\n")
			
		output_file.close()
		  
	
	  
