#    futurerank_author_reduce_two.py - Second step reducer on author score calculation
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


# Reduce phase two for calculating author scores

# ---- Imports ---- #

import sys

# ----------------- #


# ---- Initialisations ---- #

key = "start"
prev_key = "prev"
author_score = 0
current_data_dict = dict()

# ------------------------- #

# ---- Main Program / Reducer ---- #

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()
	# Get key and value - author/score - split on tab to keep author name intact
	key, val = line.split("\t")
	if(val[0] == "<"):
		current_data_dict[key] = val
		continue
	
	# ---------------------------------------------------------------- #
	
	# Key is the same
	if(key == prev_key):
		author_score += float(val)
	
	# ---------------------------------------------------------------- #
	
	# Key changed and first one
	elif(prev_key == "prev"):
		prev_key = key
		author_score += float(val)
	
	# ---------------------------------------------------------------- #
	
	# Key changed - not the first key
	else:
		current_data = current_data_dict[prev_key]
		current_data = current_data.replace("<", "")
		current_data = current_data.replace(">", "")
		author_pubs, previous_author_score = current_data.split("|")
		# TODO: Try author/author_pubs in paper and author ranking. Paper = author/author_pubs, author=paper/author_papers
		print prev_key + "\t" + author_pubs + "|" + str(author_score) + "\t" + previous_author_score
		# Re-initialise stuff
		current_data_dict.pop(prev_key, None)
		prev_key = key
		current_data = ""
		author_score = 0
		author_score += float(val)

# Print data for last key
if(key == prev_key):
	current_data = current_data_dict[key]
	current_data = current_data.replace("<", "")
	current_data = current_data.replace(">", "")
	author_pubs, previous_author_score = current_data.split("|")
	print key + "\t" + author_pubs + "|" + str(author_score) + "\t" + previous_author_score

# -------------------------------- #	
