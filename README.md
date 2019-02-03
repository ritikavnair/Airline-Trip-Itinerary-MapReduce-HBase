# Airline-Trip-Itinerary-MapReduce-HBase

This project aims to find 3-hop itineraries with a constraint of spending at least 10 hours in each city, using the flight dataset from https:/www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time. 

Our goal was to compare and contrast the performance of joins using RS Join versus HBase for computation of the best 3 hop itinerary for a traveler between all possible sources and destinations using an Airline Dataset.


Our analysis from this could help travelers (like explorers, bloggers etc.) who want to visit 3 different locations between a source and destination, plan out the best itinerary possible with the least possible trip duration. We have included constraints while calculating results so that the traveler can spend a minimum of 10 hours and a maximum of 72 hours in each city.


Additionally, we also ran queries on the data stored on HBASE for getting insightful information: Find the busiest airport in terms of number of flights, number of flights delayed per airline and average delay per month per airport etc. This analysis could be used to point out which cities would need an additional airport to divert some traffic.



Team Members:
Shubham Rastogi 
Abhishek Ahuja
Ritika Nair
