Sensoriclife - topology
=====================

Storm topology of the sensoriclife project

Build with Maven:
mvn clean assembly:assembly


Run on a cluster:
storm jar topology-VERSION-jar-with-dependencies.jar org.sensoriclife.topology.App [confs]

* -conf <path>: if given the config file will be used instead of the one in the jar
* --world: if given a new world will be generated THE OTHER GENERATORS WILL NOT RUN!!!
* --create-tables: if given a create table statement will be generated for the two tables specified in the config file


To start a new generator and generate the worlds:
storm jar topology-VERSION-jar-with-dependencies.jar org.sensoriclife.topology.App --world --create-tables


After the world is created and the topology is killed to start the smart meter generators:
storm jar topology-VERSION-jar-with-dependencies.jar org.sensoriclife.topology.App
