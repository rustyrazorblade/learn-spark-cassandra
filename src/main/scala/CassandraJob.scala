/**
 * Created by jhaddad on 9/18/14.
 *
 * Based off Ryan Svihla's CassandraCapable trait
 * https://github.com/rssvihla/spark_bulk_ops/blob/master/src/main/scala/CassandraCapable.scala
 */
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext,SparkConf}

import com.datastax.spark.connector._

trait CassandraJob {
  def connectToCassandra() : CassandraContext = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    val sc = new SparkContext("local", "test", conf)

    val connector = CassandraConnector(conf)

    new CassandraContext(connector, sc)
  }
}

class CassandraContext(val connector: CassandraConnector,
                       val sparkContext: SparkContext)
