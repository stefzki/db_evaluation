# DB Evaluation Artifact

## Purpose

Just a small helper, that allows inserting a wikipedia xml abstract dump into databases.

## Prerequisites

Download a xml abstract dump from http://dumps.wikimedia.org/enwiki/20121001/enwiki-20121001-abstract.xml and an installed JDK 20 with Maven 2/3.

## Usage

Within your ide create a run configuration with the following program parameters for the class de.strud.ImportRunner
```
--file <path to xml dump> 
--host <db host> 
--port <port>
--mode <target importer>
```

If you are not using an ide and want to start the example from your command line, run mvn clean install from the command line. If the build is successful you can start the import by the following command from the target directory:

```
java -cp "db-evaluation-0.1.0-SNAPSHOT.jar:$(echo deps/*.jar | tr ' ' ':')" de.strud.ImportRunner --file /tmp/enwiki-20121001-abstract.xml --mode=<target importer> --host=<db host> --port=<port>
```

### MongoDB

For the MongoDB example from the MongoDB User Group Berlin, the parameters should look like this:

```
--file /tmp/enwiki-20121001-abstract.xml 
--host=configsrv.local
--port=27019
--mode=mongo
```

### MySQL

```
--file /tmp/enwiki-20121001-abstract.xml
--host=mysql01.local
--port=3306
--mode=mysql
```

### Postgresql

```
--file /tmp/enwiki-20121001-abstract.xml
--host=postresql01.local
--port=5432
--mode=postresql
```

### Redis

```
--file /tmp/enwiki-20121001-abstract.xml
--host=redis01.local
--port=6379
--mode=redis
```

### Elasticsearch (to be tested)

```
--file /tmp/enwiki-20121001-abstract.xml
--mode=elasticsearch
```

### in the future

- Hadoop
- HBase
- BigCouch
- Solr
- SolrCloud