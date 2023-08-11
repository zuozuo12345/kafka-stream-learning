# IT5100B 2023 - Project 2

## Problem Statement

Consider the following experiment. A bee population (10 - 100 individuals) is fitted
with sensors and confined in a rectangular area of size `W x H` length units. The bees fly from
(say) flower to flower (we consider the flowers to have zero height), 
alternating between periods of flight and periods of being
at rest. The sensors are able to sense when the bee has landed and
is at rest, and sends a wireless signal to a central receiver. The sensors cannot
sense the resumption of flight, since that event is not of interest to us.

The sensor signal emitted when a bee lands
is converted by the central receiver into a piece of data with the
following attributes:
  * bee's id (as a string -- preferably a GUID)
  * the current timestamp, expressed as *epoch* time (i.e. number of seconds since
     January 1, 1970, 00:00 GMT)
  * `x` and `y` coordinates of the landing spot

To define the objectives of this problem, we will introduce a few conventions:

  * The rectangular area of confinement is divided into squares of `1 x 1` each. 
    Each square is identified by the pair of coordinates corresponding to its
    bottom-left corner.

  * Moreover, time is divided into `T`-second intervals (or *windows*); 
    each window ends on a timestamp that is a multiple of `T`.
    For instance, if `T` is 900 (i.e. 15 min), then there would be a window ending
    at timestamp 1648054800, another one ending at 1648055700 (= 1648054800 + 900), 
    and so on.

  * A bee that has landed on more than `K` squares is a *long-distance traveller*.


Considering `W`, `H`, `T` and `K` as inputs to our problem, the objectives of
this project are as follows:

  * Compute the number of bees that have landed in each square during each time interval.
    At the end of each time window, publish the number of bees for each square on a given
    topic (see the implementation section).
    The computation has to appear to be performed in *real-time*, that is, the output 
    corresponding to each time window has to happen as soon as possible after the window's 
    expiration time.

  * Detect the long-distance travellers and publish their IDs on a topic. Also save these
    IDs into a table in a database.

## Implementation

Assume that the central receiver publishes the landing
event data on a Kafka topic called `events`. Each published
event has the bee id, the landing timestamp (rounded down to the nearest second), 
and the coordinates of the landing point, all in CSV format.

  * **Task 1:** simulate the bees' landing events and publish them on the `events` topic on Kafka.

    * Start with a timestamp close/equal to the current time.
    * Generate random bee ID, `x` and `y` coordinates, within the given limits.
    * Publish the event (ID, timestamp, `x`, `y`) on the `events` topic.
    * Advance the timestamp by a random amount of time (possibly 0).
    * Repeat from second step.
    * Let this process run forever.
    * Place the code into the `GenerateBeeFlight.scala` file.

  * **Task 2:** using the `KStreams` DSL, implement a pipeline that computes
    the number of bee landings in each square, for each time window.

    * You may find the concept of *window* and *window aggregation* useful here.
    * You may define auxiliary topics if you feel they would be helpful.
    * Publish your output counts to the topic `bee-counts`.
    * Place your code into the `CountBeeLandings.scala`.

  * **Task 3:** using the `KStreams` DSL, implement a pipeline that detects
    long-distance travellers.

    * Publish long-distance travellers on the `long-distance-travellers` topic.
    * You must publish such travellers exactly once, as soon as they are detected.
    * Place your code into the `LongDistanceFlyers.scala` file.

  * **Task 4:** save all the long-distance travellers into the table `longdistancetravellers`
    in a Postgres DB.

    * Place your code into the `SaveLongDistanceFlyers.scala` file.

Do not forget to write tests that prove that your code is working properly.
