# CheaperTravelling
Project developed with Kafka and Spark that uses public APIs such as SkyScanner, GeoDB Cities and Flixbus to consider more more ways of travelling in a cheaper way. 

## Problem statement
Nowadays, there are many websites that help finding the best route to fly from one place to another ,such as Kayak, Skyscanner or Google Flights. However, none of these platforms take into account alternate routes with other methods of transport, such as travelling by boat, bus or train.

One use case would be to find the cheapest way to go from Stockholm to Helsinki. Searching in any of these platforms would return the best flight combination to go there, maybe even considering further airports in the same city. However, if doing a custom route that consists of taking a boat from Stockholm to Turku and then a bus to Helsinki, despite being much longer, it would result in a significantly cheaper trip.

In this project the aim is to work with different streaming data APIs to implement an algorithm that, taking into account the origin and destination of the trip and optionally some desired budget or maximum time, could find the best option for the user, recommending different itineraries.

For instance, in [this route](https://www.omio.com/search-frontend/results/EC8DCF32BAE9E485686BF274DB80DA757/ferry?locale=en&abTestParameters=&arrival_fk=388317&departure_date=04/10/2020&departure_fk=388498&passengerages%5B0%5D=26:57:&request_partner_id=&spa=&travel_mode=train&user_currency=EUR&user_domain=com&user_id=0e92f6ec-82a5-4e68-93e1-cdfa0410b5b6&user_locale=en) from Stockholm to Helsinki the comparator does not take into account going to Turku by ferry and then by bus-train to Helsinki.

## Authors
- Serghei Socolovschi [serghei@kth.se](mailto:serghei@kth.se)
- Angel Igareta [alih2@kth.se](mailto:alih2@kth.se) 
