import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._

object App extends Context {

  override val appName: String = "Spark_4_3_1"

  def main(args: Array[String]) = {

    val sc = spark.sparkContext
    implicit val jsonDecoder: Decoder[AmazonProduct] = deriveDecoder[AmazonProduct]

    sc.textFile("src/main/resources/amazon_products.json")
      .map(values => decode[AmazonProduct](values).toOption)
      .filter(result => result.isDefined)
      .map(result => result.get)
      .foreach(println(_))

  }

}




