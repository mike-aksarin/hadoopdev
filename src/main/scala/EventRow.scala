import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

/** Case class for parsing Event's CSV by spark dataframes
  */
case class EventRow(
                     product_name: String,
                     product_price: BigDecimal,
                     purchase_date: Timestamp,
                     product_category: String,
                     client_ip: String
                   )

object EventRow {
  implicit val encoder: Encoder[EventRow] = Encoders.product[EventRow]
  val schema = encoder.schema
}