import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{OverflowStrategy, DelayOverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import scala.concurrent.duration._
import scala.util.Random

case class SourceEvent(id: Integer)
case class DomainEvent(id: Integer, processingTimestamp: Long)
case class Metric(label: String, value: Int)

object Example {
  def slowSink =
  Flow[Seq[Metric]]
    .buffer(1, OverflowStrategy.backpressure)
    .delay(10 seconds, DelayOverflowStrategy.backpressure)
    .to(Sink.foreach(e => println(e)))

  def fastSource: Source[SourceEvent, NotUsed] =
    Source
      .fromIterator(() => Iterator.from(1, 1))
      .map { i =>
        println(s"Producing event $i")
        SourceEvent(i)
      }

  def enrichWithTimestamp =
    Flow[SourceEvent]
      .map { e =>
        println(s"Enriching event ${e.id}")
        DomainEvent(e.id, System.currentTimeMillis())
      }

  def countStage =
    Flow[DomainEvent]
      .grouped(100)
      .map { seq => Seq(Metric("count", seq.size)) }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Example")
    implicit val materializer = ActorMaterializer()

    fastSource
      .via(enrichWithTimestamp)
      .via(countStage)
      .to(slowSink)
      .run()
  }
}
