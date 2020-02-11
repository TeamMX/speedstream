import org.mongodb.scala.bson.conversions.Bson
import java.util.Date
import org.mongodb.scala.bson._

class SpeedUpdateBsonFactory(halfLifeMs: Int) {
    def getBson(timestamp: Long, speedKph: Double): Bson = {
        val rate = 0.5
        val factor = 1.0 / halfLifeMs
        val bTimestamp = BsonInt64(timestamp)
        val thing = operator("$pow",
            BsonDouble(rate),
            operator("$multiply",
                BsonDouble(factor),
                operator("$subtract",
                    bTimestamp,
                    operator("$ifNull",
                        BsonString("$timestamp"),
                        bTimestamp))))
        new BsonDocument("$set", new BsonDocument()
            .append("timestamp", bTimestamp)
            .append("weight", operator("$add",
                BsonInt32(1),
                operator("$multiply",
                    operator("$ifNull", BsonString("$weight"), BsonInt32(0)),
                    thing)))
            .append("value", operator("$add",
                BsonDouble(speedKph),
                operator("$multiply",
                    operator("$ifNull", BsonString("$value"), BsonInt32(0)),
                    thing))))
    }

    private def operator(operator: String, operands: BsonValue*) = {
        new BsonDocument(operator, array(operands: _*))
    }

    private def array(items: BsonValue*) = {
        val result = BsonArray()
        items.foreach(result.add)
        result
    }
}
