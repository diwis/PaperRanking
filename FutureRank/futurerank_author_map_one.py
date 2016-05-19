#    futurerank_author_map_one.py - 1st map phase for author score calculation
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

# ----------------- #

# ---- Main Program / Mapper ---- #

# Input should be <author - paper> and <paper - score> files

# Read input line by line
for line in sys.stdin:
	# Remove whitespace
	line = line.strip()

	# Check if we have an author, or publication
	
	# Case of publication!
	# NOTE: authors should NOT start with digits and papers should ONLY start with digits
	if(line[0].isdigit()):
		line_parts = line.split()
		pub_id = line_parts[0]
		pub_data = line_parts[1]
		pub_score = pub_data.split("|")[-1]
		print pub_id + "\t" + pub_score
	# Case of author
	else:
		# Split on tab character (there may be simple whitespaces in author names)
		line_parts = line.split("\t")
		# Get author name
		author = line_parts[0]
		# Get author's papers
		author_pub_data = line_parts[1]
		author_pub_data = author_pub_data.split("|")
		author_pubs = author_pub_data[0]
		author_pubs_list = author_pubs.split(",")
		for author_pub in author_pubs_list:
			pub_id, author_num = author_pub.rsplit("/", 1)
			print pub_id + "\t" + author + "//" + author_num
		# Also print author data in total
		print author + "\t<" + line_parts[1] + ">"
	
# ------------------------------- #	
