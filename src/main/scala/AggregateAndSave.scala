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
  select dateOf(k), total from stats ;

 */

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext,SparkConf}

import com.datastax.spark.connector._


object AggregateAndSave {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    val sc = new SparkContext("local", "test", conf)
    val connector = CassandraConnector(conf)

    val rdd = sc.cassandraTable("tutorial", "demo")
    val result = rdd.count()

    val kvals = rdd.map(s => s.getInt("v"))

    val ksum : java.lang.Integer = kvals.reduce((a,b) => a + b)

    connector.withSessionDo(session => {
      val prepared = session.prepare("INSERT INTO tutorial.stats (k, total) VALUES (now(), ?)")
      val bound = prepared.bind(ksum)
      session.execute(bound)
    })

    println("result", result)
    println("ksum", ksum)



  }

}
