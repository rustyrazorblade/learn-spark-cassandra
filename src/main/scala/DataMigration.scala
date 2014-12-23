/**
 * Created by jhaddad on 9/18/14.
 * we're going to read everything out of our original demo table
 * and write it to a new table in a new format
 * we're going to create an index so we can look up id based on v value
 *
 * Original table + sample data:

   CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

   CREATE TABLE tutorial.demo (
    id int PRIMARY KEY,
    v int
    );

  cqlsh:tutorial> insert into demo (id, v) values (2, 2);
  cqlsh:tutorial> select * from demo;

   id | v
  ----+-----
    1 |   2
    2 |   2
    4 |   4
    3 | 384

  (4 rows)

  The new table structure is

  create table demo_index
    ( v int, id int, primary key (v, id));

  We expect to have a partition for each value 'v':

   cqlsh:tutorial> select * from demo_index;

   v   | id
  -----+----
     2 |  1
     2 |  2
     4 |  4
   384 |  3

  (4 rows)

  cqlsh:tutorial> select * from demo_index where v = 2;

   v | id
  ---+----
   2 |  1
   2 |  2

  (2 rows)

 */

import com.datastax.spark.connector.cql.CassandraConnector

import org.apache.spark.{SparkContext,SparkConf}

import com.datastax.spark.connector._

object DataMigration extends CassandraJob {
  def main(args: Array[String]): Unit = {
    val context = connectToCassandra()
    val sc = context.sparkContext


    case class DemoIndex(v: Int, id: Int)

    val demo_table = sc.cassandraTable("tutorial", "demo")

    val collection = demo_table.map(r => new DemoIndex(r.getInt("v"), r.getInt("id")))
    collection.saveToCassandra("tutorial", "demo_index")

  }

}
