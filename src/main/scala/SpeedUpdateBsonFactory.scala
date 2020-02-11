import org.mongodb.scala.bson.conversions.Bson
import java.util.Date
import org.mongodb.scala.bson._

class SpeedUpdateBsonFactory(rate: Double, factor: Double) {
    def getBson(timestamp: Date, weight: Double, value: Double): Bson = {
        val bTimestamp = new BsonTimestamp(timestamp.getTime())
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
                BsonDouble(weight),
                operator("$multiply",
                    operator("$ifNull", BsonString("$weight"), BsonInt32(0)),
                    thing)))
            .append("value", operator("$add",
                BsonDouble(value),
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
