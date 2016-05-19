#    futurerank_author_reduce_one.py - First step reducer on author score calculation
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

# Reduce first by adding each article's score to its authors


# ---- Imports ---- #

import sys

# ----------------- #

# ---- Function definition ---- # 

# Determine if a floating point number is read
def is_float(x):
	try:
		float(x)
		return True
	except:
		return False

# ----------------------------- #

# ---- Initialisations ---- #

key = "start"
prev_key = "prev_start"
# Track authors for an article
author_list = list()
article_score = 0
author_flag = False

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input by line
for line in sys.stdin:
	line = line.strip()
	# If we split on tab we get the 
	# pub id and score or author name
	key, val = line.split("\t")
	if(val[0] == "<"):
		if(not author_flag and not key[0].isdigit() and prev_key[0].isdigit()):
			for author in author_list:
				# NOTE: There was a problem with some name containing a single "/" character. 
				author_name, collaborators = author.rsplit("//", 1)
				try:
					float(collaborators)
				except:
					print >>sys.stderr,"Problem with author: ", author_name
				print author_name + "\t" + str(float(article_score)/float(collaborators))
			author_flag = True			
		print line
		continue
	
	# ---------------------------------------------------------------- #
	
	# Key has not changed
	if(key == prev_key):
		# If the value is a number, we have the score
		# Otherwise we have an author and need to add him to author list
		if(is_float(val)):
			article_score = float(val)
		else:
			author_list.append(val)
			
	# ---------------------------------------------------------------- #
	
	# Key has changed and is first key
	elif(key != prev_key and prev_key == "prev_start"):
		prev_key = key
		author_list = list()
		article_score = 0
		if(is_float(val)):
			article_score = float(val)
		else:
			author_list.append(val)		
	
	# ---------------------------------------------------------------- #
	
	# Key has changed and is not first key	
	elif(key != prev_key):
		for author in author_list:
			# NOTE: There was a problem with some name containing a single "/" character. 
			author_name, collaborators = author.rsplit("//", 1)
			try:
				float(collaborators)
			except:
				print >>sys.stderr, "Problem with author: ", author_name
			print author_name + "\t" + str(float(article_score)/float(collaborators))
		article_score = 0
		author_list = list()
		prev_key = key
		if(is_float(val)):
			article_score = float(val)
		else:
			author_list.append(val)
		
# Print output for last key
if(key == prev_key):
	# This part should have been covered on the input read
	for author in author_list:
		author_name, collaborators = author.rsplit("//", 1)
		try:
			float(collaborators)
		except:
			print >>sys.stderr, "Problem with author: ", author_name
		print author_name + "\t" + str(float(article_score)/float(collaborators))
	author_list = list()

# ------------------------- #

