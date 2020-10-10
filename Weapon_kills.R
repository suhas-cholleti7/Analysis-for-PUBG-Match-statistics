# Weapon_kills.R
#
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
# @author: Suhas Cholleti (sc3614@rit.edu)
#
# This file uses an SQL query and brings out a dataframe which contains infomations
# of number of kills which occurrred due to the use of a particular weapon.
#

#Installing necessary packages
#install.packages(RMySQL)

# Loading the necessary libraries
library(RMySQL)

# Create a database connection using the credentials root, password. 
mydb = dbConnect(MySQL(), user='root', password='password', dbname='PUBG', host='localhost')

# Formulate the query, execute and fetch the data from the object
kill_count = dbSendQuery(mydb, "SELECT Weapon.name as Weapon, COUNT(kills.killed_by) as Number_of_Kills FROM kills
JOIN Weapon ON kills.killed_by = Weapon.id GROUP BY Weapon.id ORDER BY COUNT(kills.killed_by) DESC;")

# Fetching data
kill_count_val = fetch(kill_count, n =-1)

#Displaying the data
kill_count_val