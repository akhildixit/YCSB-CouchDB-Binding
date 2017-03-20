# YCSB-CouchDB-Binding
Contains a YCSB client binding for CouchDB, implemented using Lightcouch.

<h3>Dependencies</h3>
Lightcouch 0.1.8 - http://www.lightcouch.org/

Gson 2.8.x - https://mvnrepository.com/artifact/com.google.code.gson/gson
YCSB Core 0.12.0 - https://github.com/brianfrankcooper/YCSB/tree/master/core

<h3>Installation</h3>
Download latest release of YCSB - https://github.com/brianfrankcooper/YCSB/releases/tag/0.12.0

Export CouchClient.java as JAR and move it inside lib folder inside YCSB 0.12.0 release.
Add CouchDB binding entry in YCSB's bin/bindings.properties file.
Install Apache CouchDB 2.0.0, and use regular YCSB commands like load, run with database name 'CouchDB' to load data or run workloads.
