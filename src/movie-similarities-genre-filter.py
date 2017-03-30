"""Movie recommendation system that uses cosine similarity along with genre matching."""

import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    """Import mappings between movie names and their IDs."""
    movieNames = {}
    with open("C:\SparkCourse\ml-100k\ml-100k\u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = (fields[1].decode('ascii', 'ignore'), map(int, fields[5:]))
    return movieNames


def makePairs((user, ratings)):
    """Prep data for cosine similarity computation."""
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


def filterDuplicates((userID, ratings)):
    """Remove duplicate movie pairs."""
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def filterGenre((userID, ratings)):
    """Filter movie pairs that have no matching genres."""
    movie1Genre, movie2Genre = nameDict[ratings[0][0]], nameDict[ratings[1][0]]
    for i in range(len(movie1Genre)):
        if movie1Genre[i][1] == movie2Genre[i][1]:
            return True
    return False


def computeCosineSimilarity(ratingPairs):
    """Compute movie cosine similarities."""
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("C:\SparkCourse\ml-100k\ml-100k\u.data")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Filter out movies with no matching genres
matchingMovieGenres = uniqueJoinedRatings.filter(filterGenre)


# Now key by (movie1, movie2) pairs.
moviePairs = matchingMovieGenres.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()


# Save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID][0])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID][0] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
