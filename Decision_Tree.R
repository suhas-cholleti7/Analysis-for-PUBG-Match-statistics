# Decision_Tree.R
#
# @author: Suhas Cholleti (sc3614@rit.edu)
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
#
# Fetches player stats and uses the player stats to predict if the player ended up in top 10 or not.
# The 80% of the data is used for training the model. The rest is used to test the decision tree accuracy.
#

#Installing necessary packages
#install.packages(rpart)
#install.packages(rpart.plot)
#install.packages(RMySQL)


# Loading the necessary libraries
library(rpart)
library(rpart.plot)
library(RMySQL)

# Create a database connection using the credentials root, password. 
mydb = dbConnect(MySQL(), user='root', password='password', dbname='PUBG', host='localhost')

# Build the SQL query
rs = dbSendQuery(mydb, "SELECT damage, kills, distance_ride, distance_walked, 
                 if(((placement - 1 ) * size + 1) <= 10 , 'true', 'false') as is_top_ten 
                 FROM pubg.playerstats as p, pubg.team as t where p.player_id = t.player_id and p.game_id = t.game_id;")

# Fetching the data from the query result object
data = fetch(rs, n =-1)

# Setting the Training size. 80% is taken as training data. 20% is set aside for testing
smp_size <- floor(0.80 * nrow(data))
set.seed(123)

# training the algorithm with the fetched data and sample size
train_ind <- sample(seq_len(nrow(data)), size = smp_size)
train <- data[train_ind, ]

# testing the data
test <- data[-train_ind, ]
dts <- rpart(is_top_ten~., train, method = "class")

# Decision tree generated based on the given training data and sample size
rpart.plot(dts)

# Running Predictive analysis on test set
p<-predict(dts, test, type="class")
table(test[,5], p)