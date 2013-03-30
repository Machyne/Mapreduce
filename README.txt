This was the final mapreduce project submission from my Parallel and Distributed Computing class in the Winter of my Sophomore year (2013) at Carleton College.

For this project, we were given large data sets of impression logs and click logs. These represent data from online advertisement servers. Every time an advertisement is displayed to a customer, an entry is added to the impression log. Every time a customer clicks on an advertisement, an entry is added to the click log.

The goal of this project was to build a summary table of click through rates, which could later be queried by ad serving machines to determine the best ad to display. This table was to be a sparse matrix with the axes page URL and ad id. The value represents the percentage of times an ad was clicked. (a number between 0 and 1 inclusive)

The log files of the data are stored in JSON format, with one JSON object per line. The analysis was done using Hadoop Mapreduce.


For more information on mapreduce, read this paper: <http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/archive/mapreduce-osdi04.pdf>