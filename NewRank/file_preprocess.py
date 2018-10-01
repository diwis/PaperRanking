# 	 file_preprocess.py - Preprocess input file for NewRank's calculations
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
import sys
import math

# -------------------------------------------------------------------- #

# Functions
def get_rho(current_year, pub_year, rho):
	age = int(current_year) - int(pub_year)
	exponent = -(float(age)/float(rho))
	return math.exp(exponent)
	

# -------------------------------------------------------------------- #
	
def get_rho_dict(input_file, current_year, rho):
	rho_dict = dict()
	with open(input_file) as f:
		for line in f:
			line = line.strip()
			pub, data, prev_score, year = line.split()
			if pub not in rho_dict:
				rho_dict[pub] = get_rho(current_year, year, rho)
		
	return rho_dict
			
# -------------------------------------------------------------------- #			
	

# -------------------------------------------------------------------- #

# Read input
input_file	 = sys.argv[1]
exponent 	 = sys.argv[2]
current_year = sys.argv[3]

# Variables to use

# Store already calculated rho values here
rho_dict	 = get_rho_dict(input_file, current_year, exponent)
rho_sum		 = sum(rho_dict.values())
'''
for pub in rho_dict:
	print pub + "\t" + str(rho_dict[pub])
'''
print >>sys.stderr, "Rho sum: ", rho_sum
# -------------------------------------------------------------------- #

with open(input_file) as f:
	for line in f:
		line 							= line.strip()
		pub, pub_data, prev_score, year = line.split()
		pub_list, pub_num, score 		= pub_data.split("|")
		initial_score					= float(rho_dict[pub]/float(rho_sum))
		# Fix pub data with initial scores
		pub_data 						= pub_list + "|" + pub_num + "|" + str(initial_score)
		
		
		if pub_list == "0":
			print pub + "\t" + pub_data + "\t0\t" + year + "\t" + str(rho_dict[pub]) + "\t" + str(initial_score)
		
		else:
			pub_list = pub_list.split(",")
			pub_list_with_rho = [cited_pub + "~" + str(rho_dict[cited_pub]) for cited_pub in pub_list]
			
			print pub + "\t" + ",".join(pub_list_with_rho) + "|" + pub_num + "|" + str(initial_score) + "\t0\t" + year + "\t" + str(rho_dict[pub]) + "\t" + str(initial_score)
				
	
	

