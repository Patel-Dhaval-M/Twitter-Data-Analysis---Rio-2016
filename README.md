# Twitter-Analysis-with-Map-reduce

To analyse a dataset of Tweets collected during the 2016 Rio Olympic Games

The dataset contains a large collection of Twitter messages captured during the Rio 2016 Olympics period. The messages were collected by connecting to Twitter Streaming API, and filtering only messages directly related to the Olympic Games (by requesting they include a related hashtag such as #Rio2016 or #rioolympics ).

The input of MapReduce job will receive as Text value the whole contents for a single entry (tweet), in the format:

epoch_time;tweetId;tweet(including #hashtags);device

epoch_time provides the time the message was published, expressed in miliseconds since 01-01-1970.

tweetId is a unique identifier per message.

tweet includes the message itself. hashtags are in the tweets, identified by the hash symbol.

device provides additional meta-information, including the type of device/app used to submit the message, and a shortened url to access the message.

An example entry for the dataset is:

1469453965000;757570957502394369;Over 30 million women footballers in the world. Most of us would trade places with this lot for #Rio2016 https://t.co/Mu5miVJAWx;Twitter for iPhone

--Data Cleaning--

The provided dataset is the direct output from a Twitter crawler, and it has to be sanitised.

Filtering is done making sure only to process lines that conform to the expected format. (that is, a String that has N parts separated by a ';'. Also, There are numerous tweets written in foreign languages, which contain characters with non-standard encoding that might cause some unexpected (i.e. too high) values. This is a common occurrence when dealing with real data. Thus, tweets are filtered out with a length longer than 140 characters.

-- Part A -- Creating a Histogram plot that depicts the distribution of tweet sizes (measured in number of characters) among the Twitter dataset. To make the data more readable, the histogram aggregate bars in groups of 5 (that is, first bar counts tweets of length 1-5, second bar counts tweets 6-10, and so on) as part of MapReduce job.

-- Part B -- Creating a bar plot showing the number of Tweets that were posted each hour of the event.

-- Part C -- For the most popular hour of the games, top 10 hashtags are computed that were emitted during that hour.

For Part D & Part E - An additional secondary dataset is used, containing the list of Rio 2016 athletes that obtained a medal, and their discipline. The data is saved in Comma(,)-separated-values format, with the first row providing header names. Dataset has been downloaded from https://www.kaggle.com/rio2016/olympic-games/data . The dataset has been cropped to only include medalists in an effort to reduce the computation time it will impose to the cluster.

-- Part D -- Drawing a table with the top 30 athletes in number of mentions across the dataset.

-- Part E -- Drawing a table with the top 20 sports according to the mentions of olympic athletes captured.
