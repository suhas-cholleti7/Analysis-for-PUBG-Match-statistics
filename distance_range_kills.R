# distance_range_kills.R
#
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
# @author: Suhas Cholleti (sc3614@rit.edu)

# This file executes an SQL query and fetches a dataframe which when plotted 
# gives out information of kills based on distance range. PUBG has three range 
# specifications on kills. They are close range kills where the killer and 
# victim are ~50m separated, the mid range implies a range of 60m to ~300m. 
# Whereas the distant range kills involve a distance range of >300m. 
# This file outputs a bar plot graph with number of kills in millions.

#Installing necessary packages
#install.packages(RMySQL)

# Loading the necessary libraries
library(RMySQL)

# Create a database connection using the credentials root, password. 
mydb = dbConnect(MySQL(), user='root', password='password', dbname='PUBG', host='localhost')

# Formulate the query, execute and fetch the data from the object
short_range = dbSendQuery(mydb, "SELECT COUNT(Distance) as Short_Count
FROM 
(SELECT ROUND(SQRT(POWER(victim_position_x - killer_position_x, 2) + POWER(victim_position_y - killer_position_y, 2))/125) AS Distance FROM kills ORDER BY Distance DESC) AS Short_Count
WHERE Distance <= 50;")
short_range_val = fetch(short_range, n =-1)

# Formulate the query, execute and fetch the data from the object
mid_range = dbSendQuery(mydb, "SELECT COUNT(Distance) as Mid_Count FROM 
(SELECT ROUND(SQRT(POWER(victim_position_x - killer_position_x, 2) + POWER(victim_position_y - killer_position_y, 2))/125) AS Distance 
FROM kills ORDER BY Distance DESC) AS Mid_Count
WHERE 50 < Distance < 300;")
mid_range_val = fetch(mid_range, n =-1)

# Formulate the query, execute and fetch the data from the object
distant_range = dbSendQuery(mydb, "SELECT COUNT(Distance) as Distant_Count FROM 
(SELECT ROUND(SQRT(POWER(victim_position_x - killer_position_x, 2) + POWER(victim_position_y - killer_position_y, 2))/125) AS Distance 
FROM kills ORDER BY Distance DESC) AS Distant_Count
WHERE Distance > 300;")
distant_range_val = fetch(distant_range, n =-1)

# Creating a dataframe
show_me = data.frame(short_range_val, mid_range_val, distant_range_val)
# Creating a bar plot using the generated datafrane.
barplot(as.matrix(show_me), ylab= "Player Kill Count (In million)",
        beside=TRUE, col="lightblue")
