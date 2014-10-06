Requirements
=============

The most recent version of Cassandra 2.0.

Ideally an IDE that supports Scala.

Scala + SBT


Content
=========

Each example should be readable with minimal outside references.  They are meant to be read through (and tried out) in order, since they build on each other.


Examples
========

1. AggregateAndSave

Demonstrates simple connection and aggregation.  Hard coded for a dev environment, connecting to localhost.


2. DataMigration

Here's an introduction to doing a data migration between Cassandra tables using Spark.  To save time, a trait is used to get the context to avoid having to copy and paste code everywhere.  This trait will be used by several projects in the future.



