#    futurerank_error_map.py - Mapper for FR max error calculation
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

# Mapper for error calculation

# ---- Imports ---- #

import sys
import math

# ----------------- #

# ---- Main Program / Mapper ---- #

# Read input line by line and calculate rank score difference squared
for line in sys.stdin:
	line = line.strip()
	# No matter if we have author or paper, scores will be situated at the same place
	line_parts = line.split("\t")
	line_data = line_parts[1]
	current_score = line_data.split("|")[-1]
	previous_score = line_parts[2]
	score_difference = float(current_score) - float(previous_score)
	score_difference_squared = score_difference ** 2
	print score_difference_squared

# ------------------------------- #
