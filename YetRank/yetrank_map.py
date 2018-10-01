#    yetrank_map.py - YetRank basic step mapper
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

# -------------------------------------------------------------------- #

# NOTE: according to the paper, we don't output scores divided by 
# citations, but complete scores. Commented out will be the option
# to do it weighted, in PageRank fashion

for line in sys.stdin:
	line = line.strip()
	pub, pub_data, prev_score, pub_year, pub_weight = line.split()
	
	print pub + "\t<" + pub_data + "|" + prev_score + "|" + pub_year  + "|" + pub_weight + ">"
 	print pub + "\t0"
 	
 	cited_list, cited_num, score = pub_data.split("|")
 	cited_list = cited_list.split(",")
 	
 	if cited_list == ["0"]:
		continue
	
	# This does not help convergence
	'''
	for cited_pub in cited_list:
		print cited_pub + "\t" + score
	'''
	# This is a pageRank style score transfer w/ scores distributed to all outlinks
	for cited_pub in cited_list:
		print cited_pub + "\t" + str(float(score)/float(cited_num))
