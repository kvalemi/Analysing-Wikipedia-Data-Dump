## Project Description

In this mini project, I aimed to build a PySpark pipeline that parses the giant Wikipedia data dump (~50GB) and provides us with some useful analysis. The actual zipped data files are stored on SFU's Hadoop cluster, so we will have to remotely run our scripts for testing. My plan is to build a pipeline that answers the following question:

1) What is the most viewed page each hour of the day over all of Wikipedia?

## Building the Project

The sample data files located in this repo are there only for testing the scripts locally if one has Hadoop locally. Otherwise, we have to run the script on some remote Hadoop cluster.

1) to answer question 1: `spark-submit wikipedia_popular.py pagecounts-3`
