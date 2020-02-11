import org.mongodb.scala.{MongoCollection, Document}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.bson.conversions.Bson
import java.util.Date

case class Speed(from: String, to: String, timestamp: Long, speedKph: Double)

class MongoSpeedUpdater(
    collection: MongoCollection[Document],
    factory: SpeedUpdateBsonFactory) {

    def accept(entry: Speed) = {
        val filter = getFilter(entry)
        val update = getUpdate(entry)
        val options = new UpdateOptions().upsert(true)
        collection
            .updateOne(filter, update, options)
            .toFuture()
    }

    private def getFilter(entry: Speed) = {
        equal("_id", s"${entry.from} ${entry.to}")
    }

    private def getUpdate(entry: Speed): Seq[Bson] = {
        List(factory.getBson(entry.timestamp, entry.speedKph))
    }
}
