#    yetrank_reduce.py - YetRank basic step reducer
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

alpha 			= float(sys.argv[1])
dangling_sum	= float(sys.argv[2])
previous_key 	= ""
yetrank_sum 	= 0.0
pub_data_dict 	= dict()

# -------------------------------------------------------------------- #

for line in sys.stdin:
	
	'''
	print "Line:", line.strip()
	print pub_data_dict
	print
	'''
	
	line = line.strip()
	key,val = line.split()
	
	# If first loop
	if (previous_key == ""):
		previous_key = key
		
		if val.startswith("<"):
			pub_data_dict[key] = val
		else:
			yetrank_sum += float(val)
			
	# If key is unchanged
	elif (previous_key == key):
		if val.startswith("<"):
			pub_data_dict[key] = val
		else:
			yetrank_sum += float(val)
			
	elif (previous_key != key):
		
		# Output stuff for previous key
		pub_list, pub_num, yetrank, prev_yetrank, pub_year, pub_weight = pub_data_dict[previous_key].replace("<", "").replace(">", "").split("|")
		# Remove from data dictionary
		pub_data_dict.pop(previous_key)		
		# Calculate new yetrank score
		new_yetrank = alpha * (yetrank_sum + dangling_sum) + (1-alpha) * (float(pub_weight))
		# Print output
		print previous_key + "\t" + pub_list + "|" + pub_num + "|" + str(new_yetrank) + "\t" + yetrank + "\t" + pub_year + "\t" + pub_weight
		
		# Re-initialize
		yetrank_sum = 0.0
		previous_key = key
		
		# Do stuff for new key		
		if val.startswith("<"):
			pub_data_dict[key] = val
		else:
			yetrank_sum += float(val)
	

# Do output for final value
pub_list, pub_num, yetrank, prev_yetrank, pub_year, pub_weight = pub_data_dict[previous_key].replace("<", "").replace(">", "").split("|")
# Remove from data dictionary
pub_data_dict.pop(key)		
# Calculate new yetrank score
new_yetrank = alpha * (yetrank_sum + dangling_sum) + (1-alpha) * (float(pub_weight))
# Print output
print key + "\t" + pub_list + "|" + pub_num + "|" + str(new_yetrank) + "\t" + yetrank + "\t" + pub_year + "\t" + pub_weight

		
		
