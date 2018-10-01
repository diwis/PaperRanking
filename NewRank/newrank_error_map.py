# 	 newrank_error_map.py - Get max error: map step
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


import sys
import math

# -------------------------------------------------------------------- #
max_error = 0.0

for line in sys.stdin:
	line_parts 			= line.strip().split()
	# Get parts with scores
	pub_data 			= line_parts[1]
	previous_newrank	= line_parts[2]
	# Get current score
	current_newrank		= pub_data.split("|")[-1]
	# Get change in score
	score_diff = math.fabs(float(current_newrank) - float(previous_newrank))
	if score_diff > max_error:
		max_error = score_diff
		
print max_error
	
