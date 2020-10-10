# heat_map.R
#
# @author: Suhas Cholleti (sc3614@rit.edu)
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
#
# In this file, we generate the data from our database that is related the player
# coordinates. Using the query we get the necessary data and using a scaling
# factor we change the coordinate surroundings by this factor elevating the
# region to conjugate data for a heat map.
#

#Installing necessary packages
#install.packages(plotly)
#install.packages(RMySQL)

# Loading the necessary libraries
library(RMySQL)
library(plotly)

# Create a database connection using the credentials root, password.
mydb = dbConnect(MySQL(), user='root', password='password', dbname='PUBG', host='localhost')

# Formulate the query, execute and fetch the data from the object
rs = dbSendQuery(mydb, "select killer_position_x, killer_position_y from kills, game where game.id = kills.game_id and
                 map_id = 2")

# Fetch the data from the result object
data = fetch(rs, n =-1)

# Creating the scaling factor
golden_number = 1000000 / 200
heat_map = matrix(0, nrow = 200, ncol = 200)

# Elevating the surrounding coordinates of a selected coordinate
for(row in 1:nrow(data)){
  x <- as.integer(data[row, 1] / golden_number)
  y <- as.integer(data[row, 2] / golden_number)
  heat_map[x, y] = heat_map[x, y]  + 1
  heat_map[x+1, y] = heat_map[x+1, y]  + 1
  heat_map[x-1, y] = heat_map[x-1, y]  + 1
  heat_map[x, y+1] = heat_map[x, y+1]  + 1
  heat_map[x, y-1] = heat_map[x, y-1]  + 1
  heat_map[x+1, y+1] = heat_map[x+1, y+1]  + 1
  heat_map[x-1, y-1] = heat_map[x-1, y-1]  + 1
  heat_map[x-1, y+1] = heat_map[x-1, y+1]  + 1
  heat_map[x+1, y-1] = heat_map[x+1, y-1]  + 1
}

# heat_map = log2(heat_map)
# Creating a figure with grey, red complexions as a heat map
fig <- plot_ly(z = heat_map, colors = colorRamp(c("grey", "red")), type = "heatmap")
# Displaying the figure
fig