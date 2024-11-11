System Configuration:-

    Operating System:- MacOS
    Scala Version:- 2.11.7
    JDK:- OpenJdb 1.8
    Dependency Management tool:- sbt
    sbt version:- 1.8.2
    Spark Version:- 2.4.8
    Kafka Version:- 3.6.0


Steps to run the Program:-

1.) compile the project using command:- sbt compile
2.) To run the streaming pipeline use following command:- sbt "runMain Main"
3.) To run the producer use following command:- sbt "runMain Producer"



Note:- Before testing each pipeline to simulate the streaming scenario, we have written the producer program which ingests the data from text file to kafka topic.

Hence before running each algorithm make sure to delete data in topic and then start the streaming pipeline, after that run the producer program which publishes the data to kafka topic.
