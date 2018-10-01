# 	 newrank_map.py - NewRank map step
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

# -------------------------------------------------------------------- #

# -------------------------------------------------------------------- #

for line in sys.stdin:
	line = line.strip()
	# Get line parts
	pub, pub_data, prev_score, year, pub_rho, prior_rho = line.split()
	# Get outlink data and score
	pub_list, pub_num, current_score = pub_data.split("|")
	
	# Keep pub data for next steps.
	print pub + "\t" + "<" + pub_data + "|" + prev_score + "|" + year + "|" + pub_rho + "|" + prior_rho + ">"
	print pub + "\t0" 
	
	# If no outlinks, don't do anything
	if pub_num == "0":
		continue
		
	# If we have outlinks, calculate newrank to output
	pub_list = pub_list.split(",")
	rho_list = [float(cited_pub.split("~")[-1]) for cited_pub in pub_list] 
	for cited_pub_data in pub_list:
		# Get cited pub rho
		cited_pub, cited_pub_rho = cited_pub_data.split("~")
		
		# ------ This is the way it's written in the paper ----------- #
		'''
		# Output score calcualtion
		newrank_raw = float(current_score)/float(pub_num)
		# Delta prob
		delta_prob 	= float(pub_rho)/sum(rho_list)
		# Score to output is ( newrank/outlinks ) * delta
		print cited_pub + "\t" + str(newrank_raw*delta_prob)
		'''
		# ------------------------------------------------------------ #
		
		
		
		# ----- This is the way is SHOULD BE based on the paper ------ #		
		
		# Output score calcualtion
		newrank_raw = float(current_score)/float(pub_num)
		# Delta prob
		delta_prob 	= float(cited_pub_rho)/sum(rho_list)
		# Score to output is ( newrank/outlinks ) * delta
		print cited_pub + "\t" + str(newrank_raw*delta_prob)		
		
		# ------------------------------------------------------------ #	
