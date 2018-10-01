#	 newrank_reduce.py - NewRank reduce step
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

alpha			= float(sys.argv[1])
total_nodes 	= sys.argv[2]
dangling_score 	= float(sys.argv[3])

# -------------------------------------------------------------------- #

previous_key = ""
current_data_dict = dict()
running_newrank = 0.0

# -------------------------------------------------------------------- #

for line in sys.stdin:
	
	# get key and value
	key, val = line.split()
	
	# First run
	if (previous_key == ""):
		# Initialize key
		previous_key = key
		
		if val.startswith("<"):
			current_data_dict[key] = val
		else:
			running_newrank +=  float(val)
			
			
	# If we continue on same key, add ranks transferred
	elif (previous_key == key):
		if val.startswith("<"):
			current_data_dict[key] = val
		else:		
			running_newrank +=  float(val)
	
	# If key changes
	elif (previous_key != key):
		
		# First keep  previous key data
		pub_list, pub_num, current_score, previous_score, year, pub_rho, pub_prior = current_data_dict[previous_key].replace("<", "").replace(">","").split("|")
		current_data_dict.pop(previous_key)
		
		# Get paper rho / sum(rho)
		pub_prior = float(pub_prior)
		
		# Do calculations for new NewRank score
		newrank = alpha * (running_newrank +  dangling_score * pub_prior) + (1 - alpha) * (pub_prior/float(total_nodes))
		# Output pub data w new scores
		print previous_key + "\t" + pub_list + "|" + pub_num + "|" + str(newrank) + "\t" + current_score + "\t" + year + "\t" +  pub_rho + "\t" + str(pub_prior)
		
		
		# RE-initialise
		previous_key = key
		running_newrank = 0.0
		if val.startswith("<"):
			current_data_dict[key] = val
		else:		
			running_newrank +=  float(val)
		

# CALCULATIONS FOR LAST LINE/KEY
# First keep  previous key data
pub_list, pub_num, current_score, previous_score, year, pub_rho, pub_prior = current_data_dict[key].replace("<", "").replace(">","").split("|")
current_data_dict.pop(key)
	
# Get paper rho / sum(rho)
pub_prior = float(pub_prior)

# Do calculations for new NewRank score
newrank = alpha * (running_newrank +  dangling_score * pub_prior) + (1 - alpha) * (pub_prior/float(total_nodes))
# Output pub data w new scores
print key + "\t" + pub_list + "|" + pub_num + "|" + str(newrank) + "\t" + current_score + "\t" + year + "\t" +  pub_rho + "\t" + str(pub_prior)
		
				
		
		
		
	
