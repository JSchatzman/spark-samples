# spark-samples

#### This repository holds samples of spark exercises completed for learning purposes.  The data sets used come in the form of a CSV or plain text file.

### movie-similarities-genre-filter.py

##### Using a data file mapping users to movies with ratings, we are able to provide recommendations for movies that are similar to a chosen movie.  We first import the data set as key pair values in which keys are users and values are tuples in the form of (movieid, rating).  We then join this RDD to itself to find all possible movie pairs for a given user.  We filter out movie combinations in which each movie is the same or movies that have no genres in common.  Next, we transform our RDD to eliminate the userID and instead use movie combinations as keys while while pairs of ratings exist as keys.  Finally, we compute the cosine similarity for each movie pair and then print the movies that meet specified thresholds for both cooccurence and similarity score.


### sales-by-customer.py

##### This is a simple aggregation grouped by customer using a sample csv file with each row representing a retail order.

### word-count.py

##### This script intakes a text file and flattens it into an RDD.  It counts the number of occurances of each word and then displays the words in order of increasing occurences.



