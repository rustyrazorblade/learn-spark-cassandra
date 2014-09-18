/**
 * Created by jhaddad on 9/18/14.


 This is my first attempt at doing anything with spark
 We're going to create a table, put some data into it, and aggregate the values
 We can then save the results to cassandra
 We'll build this out more to get more complex jobs
 This is only meant to be used in dev, since it's hard coded to "local" (no master)

  CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

  CREATE TABLE tutorial.demo (
      id int PRIMARY KEY,
      v int
  )

  insert into demo (id, v) values (1, 2);
  insert into demo (id, v) VALUES ( 3, 384);
  insert into demo (id, v) VALUES ( 4, 4);

  create table stats ( k timeuuid primary key, total int );


  After you run the job, select from the table:

 cqlsh:tutorial> select dateOf(k), total from stats ;

 dateOf(k)                | total
--------------------------+-------
 2014-09-18 13:22:37-0700 |   390

 */

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext,SparkConf}

import com.datastax.spark.connector._


object AggregateAndSave {
  def main(args: Array[String]): Unit = {

    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "test", conf)

    // we need this later to execute queries
    // there's another way of writing data to cassandra, we'll cover it later
    // this is good for single queries
    // the future version will be for saving RDDs
    val connector = CassandraConnector(conf)

    // keyspace & table
    val rdd = sc.cassandraTable("tutorial", "demo")

    // get a simple count
    val result = rdd.count()

    // map accepts a function.  the function receives a CassandraRow
    // we return a new result for each row.  in this case, our "v" column
    val kvals = rdd.map(s => s.getInt("v"))

    // we use reduce to sum all the "v" columns we extracted in the previous map function
    val ksum : java.lang.Integer = kvals.reduce((a,b) => a + b)

    // we can get a session out of the connector to insert our data back into cassandra
    connector.withSessionDo(session => {
      val prepared = session.prepare("INSERT INTO tutorial.stats (k, total) VALUES (now(), ?)")
      val bound = prepared.bind(ksum)
      session.execute(bound)
    })

    println("result", result)
    println("ksum", ksum)

  }

}
