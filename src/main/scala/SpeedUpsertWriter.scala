import org.apache.spark.sql.ForeachWriter
import org.mongodb.scala.{MongoClient, MongoDatabase, MongoCollection, Document}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SpeedUpsertWriter(
    connectionString: String,
    databaseName: String,
    collectionName: String,
    rate: Double,
    factor: Double
) extends ForeachWriter[Speed] {

    var mongoClient: MongoClient = _
    var database: MongoDatabase = _
    var collection: MongoCollection[Document] = _
    var updater: MongoSpeedUpdater = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
        mongoClient = MongoClient(connectionString)
        database = mongoClient.getDatabase(databaseName)
        collection = database.getCollection(collectionName)
        updater = new MongoSpeedUpdater(
            collection,
            new SpeedUpdateBsonFactory(rate, factor))
        true
    }

    override def process(value: Speed): Unit = {
        try {
            Await.result(updater.accept(value), Duration.Inf)
        } catch {
            case t: Throwable => println(t)
        }
    }

    override def close(errorOrNull: Throwable): Unit = {
        // do nothing
    }
}
