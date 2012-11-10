# DB Evaluation Artifact

## Purpose

Just a small helper, that allows inserting a wikipedia xml abstract dump into databases.

## Prerequisites

Download a xml abstract dump from http://dumps.wikimedia.org/enwiki/20121001/enwiki-20121001-abstract.xml and an installed JDK 7.0 with Maven 2/3.

## Usage

Within your ide create a run configuration with the following program parameters for the class de.strud.ImportRunner
```
--file <path to xml dump> 
--host <db host> 
--port <port>
```

For the MongoDB example from the MongoDB User Group Berlin, the parameters should look like this:

```
--file /tmp/enwiki-20121001-abstract.xml 
--host=configsrv.local 
--port=27019
```

If you are not using an ide and want to start the example from your command line, run mvn clean install from the command line. If the build is successfull you can start the import by the following command from the target directory:

```
java -cp "db-evaluation-0.1.0-SNAPSHOT.jar:deps/commons-io-2.4.jar:deps/jewelcli-0.8.3.jar:deps/log4j-1.2.16.jar:deps/mongo-java-driver-2.9.1.jar:deps/hamcrest-core-1.1.jar:deps/metrics-core-2.1.3.jar:deps/slf4j-api-1.6.4.jar" de.strud.xmlparser.XMLParser --file /tmp/enwiki-20121001-abstract.xml --host=configsrv.local --port=27019
```

