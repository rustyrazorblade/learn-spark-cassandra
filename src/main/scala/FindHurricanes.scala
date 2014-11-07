import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext

import scala.util.control.NonFatal

/**
 * Created by patrick on 10/17/14.
 *
 * Looks at 55163987 local records
 *
 * CAT5:
 * wind_speed > 69 AND pressure < 944
 *
 * Any Hurricane
 * (wind_speed > 37 OR wind_speed = 0) AND pressure < 980
 * 722315:53917,NEW ORLEANS LAKEFRONT AP,US,LA,KNEW,30.05,-90.033,2.7
 *
 */
object FindHurricanes {
    def main(args: Array[String]): Unit = {
      // the setMaster("local") lets us run & test the job right in our IDE
      val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

      // "local" here is the master, meaning we don't explicitly have a spark master set up
      val sc = new SparkContext("local", "weather", conf)

      // we need this later to execute queries
      // there's another way of writing data to cassandra, we'll cover it later
      // this is good for single queries
      // the future version will be for saving RDDs
      val connector = CassandraConnector(conf)

      case class hurricane_candidates (weather_station: String,
                                       weather_station_name: String,
                                       country_code: String,
                                       state_code: String,
                                       wind_speed: Double,
                                       one_hour_precip: Double,
                                       pressure: Double,
                                       year: Int,
                                       month: Int,
                                       day: Int)


      val cc = new CassandraSQLContext(sc)
      cc.setKeyspace("isd_weather_data")

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)


      //sqlContext.cacheTable("weather_station")

      //val srdd: SchemaRDD = cc.sql("select weather_station,wind_speed,pressure,one_hour_precip,month,day from raw_weather_data where (wind_speed > 37 OR wind_speed = 0) AND (pressure < 980 AND pressure > 900);")

      //val srdd: SchemaRDD = cc.sql("SELECT weather_station, year, month, day, max(temperature) high, min(temperature) low FROM raw_weather_data WHERE weather_station = '477710:99999' AND month = 6 GROUP BY weather_station, year, month, day;");

      val selectSql = "SELECT r.weather_station, w.name, w.country_code, w.state_code, r.wind_speed, r.one_hour_precip, r.pressure, r.year, r.month, r.day " +
                      "FROM raw_weather_data r, weather_station w " +
                      "WHERE w.state_code IS NOT NULL AND w.country_code IS NOT NULL AND r.month = 8 AND (r.wind_speed > 69 OR r.wind_speed = 0) AND r.pressure < 944 AND r.weather_station = w.id;"


      val srdd: SchemaRDD = cc.sql(selectSql)

      try
        val resultSet = srdd.collect().map(row => (new hurricane_candidates(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(4), row.getDouble(5), row.getDouble(6), row.getInt(7), row.getInt(8), row.getInt(9))))


      val insertSql = "INSERT INTO hurricane_candidates (weather_station, weather_station_name, country_code, year, month, day, wind_speed, pressure, one_hour_precip)" +
                      "VALUES (?,?,?,?,?,?,?,?,?)"

      connector.withSessionDo(session => {
        val prepared = session.prepare(insertSql)
        val bound = prepared.bind
        for (row <- resultSet) {
          bound.setString("weather_station", row.weather_station)
          bound.setString("weather_station_name", row.weather_station_name)
          bound.setString("country_code", row.country_code)
          bound.setString("state_code", row.state_code)
          bound.setInt("year", row.year)
          bound.setInt("month", row.month)
          bound.setInt("day", row.day)
          bound.setDouble("wind_speed", row.wind_speed)
          bound.setDouble("pressure", row.pressure)
          bound.setDouble("one_hour_precip", row.one_hour_precip)

          session.execute(bound)
        }
      })
    }
}
