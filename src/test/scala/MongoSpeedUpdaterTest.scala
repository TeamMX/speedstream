import org.mongodb.scala.{MongoClient, MongoDatabase, MongoCollection, Document}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.Date

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.mongodb.scala.bson.BsonDocument

// note: scala tests in same suite are run in series
// note: scala tests in diff suites are run in parallel

class MongoSpeedUpdaterTest extends AnyFunSuite with BeforeAndAfterEach {
    val fortySecondsMs = 40000
    val unixTimestampMs = 200000L
    val emptyQuery = new BsonDocument()

    var isInit = false
    var collection: MongoCollection[Document] = _
    var updater: MongoSpeedUpdater = _

    override def beforeEach() {
        if (!isInit) {
            collection = MongoClient("mongodb://localhost:27017").getDatabase("capstonetest").getCollection("capstonetest")
            updater = new MongoSpeedUpdater(
                collection,
                new SpeedUpdateBsonFactory(20000))
            isInit = true
        }
        // clear the collection
        Await.result(collection.deleteMany(emptyQuery).toFuture(), Duration.Inf)
    }

    test("MongoSpeedUpdater.accept previously unknown segment") {
        // arrange
        val record = new Speed("123", "456", unixTimestampMs, 100)

        // act
        Await.result(updater.accept(record), Duration.Inf)
        val documents = Await.result(collection.find().toFuture(), Duration.Inf).toList

        // assert
        assertResult(1, "number of documents") {
            documents.length
        }
        assertResult("123 456", "_id") {
            documents(0).getString("_id")
        }
        assertResult(unixTimestampMs, "timestamp") {
            documents(0).getLong("timestamp")
        }
        assertResult(100, "speedkph") {
            documents(0).getDouble("value") / documents(0).getDouble("weight")
        }
    }

    test("MongoSpeedUpdater.accept segment of same timestamp") {
        // arrange
        val firstRecord = new Speed("123", "456", unixTimestampMs, 100)
        val secondRecord = new Speed("123", "456", unixTimestampMs, 50)
        Await.result(updater.accept(firstRecord), Duration.Inf)

        // act
        Await.result(updater.accept(secondRecord), Duration.Inf)
        val documents = Await.result(collection.find().toFuture, Duration.Inf).toList

        // assert
        assertResult(1, "number of documents"){ documents.length }
        assertResult(75, "speedkph") {
            documents(0).getDouble("value") / documents(0).getDouble("weight")
        }
    }

    test("MongoSpeedUpdater.accept segment of newer timestamp") {
        // arrange
        val firstRecord = new Speed("123", "456", unixTimestampMs, 100)
        val secondRecord = new Speed("123", "456", unixTimestampMs + fortySecondsMs, 50)
        Await.result(updater.accept(firstRecord), Duration.Inf)

        // act
        Await.result(updater.accept(secondRecord), Duration.Inf)
        val documents = Await.result(collection.find().toFuture, Duration.Inf).toList

        // assert
        assertResult(1, "number of documents") { documents.length }
        assertResult(60, "speedkph") {
            documents(0).getDouble("value") / documents(0).getDouble("weight")
        }
    }

    test("MongoSpeedUpdater.accept segment of older timestamp") {
        // arrange
        val firstRecord = new Speed("123", "456", unixTimestampMs + fortySecondsMs, 50)
        val secondRecord = new Speed("123", "456", unixTimestampMs, 100)
        Await.result(updater.accept(firstRecord), Duration.Inf)

        // act
        Await.result(updater.accept(secondRecord), Duration.Inf)
        val documents = Await.result(collection.find().toFuture, Duration.Inf).toList

        // assert        
        assertResult(1, "number of documents") { documents.length }
        assertResult(60, "speedkph") {
            documents(0).getDouble("value") / documents(0).getDouble("weight")
        }
    }

    test("MongoSpeedUpdater.accept works with real powers") {
        // arrange
        val firstRecord = new Speed("123", "456", unixTimestampMs, 100)
        val secondRecord = new Speed("123", "456", unixTimestampMs + 10000, 50)
        Await.result(updater.accept(firstRecord), Duration.Inf)

        // act
        Await.result(updater.accept(secondRecord), Duration.Inf)

        // assert
        // it didn't crash, so we're good
    }
}
