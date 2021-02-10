<h1 align="center">Smart flight comparer considering alternative transportation</h1>
<h4 align="center">Final project of the Data-Intensive Computing course of the EIT Digital data science master at <a href="https://www.kth.se/en">KTH</a></h4>

<p align="center">
  <img alt="KTH" src="https://img.shields.io/badge/EIT%20Digital-KTH-%231954a6?style=flat-square" />  
  <img alt="License" src="https://img.shields.io/github/license/angeligareta/cheaper-travelling?style=flat-square" />
  <img alt="GitHub contributors" src="https://img.shields.io/github/contributors/angeligareta/cheaper-travelling?style=flat-square" />
</p>

Project developed with Apache Spark and Kafka that works with different public streaming data APIs such as SkyScanner, GeoDB Cities and Flixbus to consider more ways of travelling in a cheaper way. 

## Problem statement
Nowadays, there are several websites that help finding the best route to fly from one place to another,
such as Kayak, SkyScanner or Google Flights. However, none of these platforms take into account
alternate routes with other methods of transport, such as travelling by boat, bus or train.

In this project the aim is to work with different data streaming APIs to implement a Spark Streaming
application that, reading from Kafka data stream the origin, destination, departure date, price and
time ranges, returns the optimal combination of routes, in terms of time and price.

## Tools
The tools utilized for this project would include:
- Spark Streaming: an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams.
- Kafka: a distributed, topic-oriented, partitioned, replicated commit log service used to publish and subscribe to streams of data.

## Data
The data is retrieved from the following APIs:
- [GeoDB Cities API](https://rapidapi.com/wirefreethought/api/geodb-cities/details): A service that provides basic information about cities and countries around the world. It allows to constrain and sort the data in various ways, such as distance or maximum population. The data, however, cannot be considered reliable, since the provided information sometimes can be contrasted by the facts found on the Internet.
- [SkyScanner Flight Search API](https://skyscanner.github.io/slate/): Cloud service offered through the API portal RapidAPI that offers different endpoints to retrieve information about the airports in different cities, and possible connections among the airports. However, the data that can be extracted present some problems:
  - In case the flight is not direct, no transit stops are indicated.
  - There is no information about the departure or arrival time and flight duration.
  - The returned data is not always reliable. To collect airports through the API a query has to be specified, but no coordinates, resulting in the return of airports from other countries with similar names.
  - Complex and obsolete JSON structure, which requires a lot of processing to extract the necessary information.
  - If the flight is not available on the selected date, API still returns options for close dates.
  - Limited number of requests per minute.
- [Meinfernbus-REST API](https://github.com/juliuste/meinfernbus-rest): Unofficial Public Flixbus API, containing the information about the bus stations in the cities and all the connections contained in the Flixbus database. It is possible to recover all the necessary information from the API replies, starting from departure and arrival stations to scheduled times and prices. The problems encountered with the data received from the API are the following:
  - Limited coverage of Flixbus, because the company is not operating in all the European countries. In order to solve this, it would be necessary to integrate other public transportation APIs.
  - Different name formats that make it difficult to recognize bus stops in the airports indicated by SkyScanner API.
       
## Developed Applications

### Search Engine

The Search Engine aims to act as a front-end to collect information about the trip the user aspires to do. First, it prompts the required information by console specifying the expected format and showing some example results. This information is encoded in a JSON with custom properties and it is sent through a producer to a Kafka topic with the key input-[userid]. After this, a Spark Streaming Context is created along with a Kafka consumer, which is subscribed to the exact same topic name but only processing the keys that start with 'output-[userid]', so the results are custom to only the user with [userid]. Finally, the top n routes if found are displayed in the console.
  
### Trip Generator Back-end

This second application is in charge of processing the input received in a custom Kafka Topic, collecting
data from the previously stated APIs to implement the idea, and finally returning the top n routes for
the user [userid]. To achieve this, the following steps were performed:
- Collect the client request from the Kafka Topic that contains [src, departureDate, dst, priceRange, timeTravelRange]
- Look for the nearest cities from the coordinates stored in src and dst in GeoDB Cities API. The nearest cities will become the origin and the destination points, while the furthest would be considered as transit points.
- Search for the transport hubs in the before-mentioned cities, such as airports and bus stations, using Skyscanner and FlixBus APIs. To achieve this, as these stations contained different names, a cluster of stations in the same city had to be manually performed.
- Find connections among the generated clusters for the departure date, considering all the stations contained in every cluster.
- Create a graph, where the cities with the relative transport hubs are considered as nodes, and the routes are the edges. These edges contain a certain time and price, which later will be filtered taking into account the input price range and travel time range.
- Look for the optimal path from the origin to the destinations, in terms of time and/or price.
  
## Results
The first image shows how the user can insert the data required to perform the query. The query is
then encoded as a JSON and sent to the Kafka topic with the keyinput-[userid].
![TODO](TODO)
  
The following screenshot presents a partial log of how the back-end processes the query.
![TODO](TODO)

Lastly, the final output of the application is presented, which shows the top 5 routes according to the
price. Unfortunately, it was not possible to perform such optimization with a precise time, because
SkyScanner API was not providing correct data regarding the flight duration.
![TODO](TODO)

## How to run code
As Kafka uses ZooKeeper to maintain the configuration information, the ZooKeeper server needs to
be started followed by the Kafka server.
```
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Next, a Kafka topic with [topicname] needs to be created, where all the messages will be exchanged.
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic [topic_name]
```
  
Regarding the applications, first the trip generator needs to be executed from the root of the source
project containing build.sbt. This can be done with the utility sbt.
```
sbt "run [topic_name]" // Select Trip Generator
```
  
Finally, the search engine needs to be executed following the same procedure as in the last step but
choosing Search Engine. After executing it, the trip details will be prompted.

## Authors
- Serghei Socolovschi [serghei@kth.se](mailto:serghei@kth.se)
- Angel Igareta [alih2@kth.se](mailto:alih2@kth.se) 
