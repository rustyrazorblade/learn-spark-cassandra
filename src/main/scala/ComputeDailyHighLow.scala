import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext


/**
 * Created by patrick on 10/17/14.
 *
 * Looks at 55163987 local records
 *
 * Compute the daily high and low temperature for each weather station
 *
 */
object ComputeDailyHighLow {
  case class daily_aggregate_temperature (weather_station: String, year: Int, month: Int, day: Int, high:Double, low:Double)

  def main(args: Array[String]): Unit = {
    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "weather", conf)

    val connector = CassandraConnector(conf)



    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("isd_weather_data")

    val aggregationSql = "SELECT weather_station, year, month, day, max(temperature) high, min(temperature) low " +
              "FROM raw_weather_data " +
              "WHERE month = 6 " +
              "GROUP BY weather_station, year, month, day;"

    val srdd: SchemaRDD = cc.sql(aggregationSql);

    val resultSet = srdd.map(row => (new daily_aggregate_temperature(row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getDouble(4), row.getDouble(5)))).collect()


    val insertStatement = "INSERT INTO isd_weather_data.daily_aggregate_temperature (weather_station, year, month, day, high, low) " +
                          "VALUES (?,?,?,?,?,?)"

    connector.withSessionDo(session => {
      val prepared = session.prepare(insertStatement)
      val bound = prepared.bind

      for (row <- resultSet) {
        bound.setString("weather_station", row.weather_station)
        bound.setInt("year", row.year)
        bound.setInt("month", row.month)
        bound.setInt("day", row.day)
        bound.setDouble("high", row.high)
        bound.setDouble("low", row.low)
        session.execute(bound)
      }
    })
  }

}

