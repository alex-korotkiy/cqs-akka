import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka_typed.CalculatorRepository.{getLatestOffsetAndResult, initDataBase, updateResultAndOfsset}
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}
import slick.jdbc.H2Profile.api._

import scala.concurrent.Await


case class Action(value: Int, name: String)


object akka_typed
{
  trait CborSerializable

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int)      extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int)   extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int)      extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int)    extends Event
    case class CalculationCompleted(id: Int, amount: Int)    extends Event

    final case class State(value: Int) extends CborSerializable {
      def add(amount: Int): State      = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State   = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }

    def apply(): Behavior[Command] =
      Behaviors.setup { ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
          .snapshotWhen{
          case (state, CalculationCompleted(_, _), seqNumber) if seqNumber % 10 == 0 => true
          case (state, event, seqNumber) => true
        }
          .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
      }

    def handleCommand(
        persistenceId: String,
        state: State,
        command: Command,
        ctx: ActorContext[Command]
    ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"Receive adding for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"Receive multiplying for number: $amount and state is ${state.value}")
          Effect
            .persist(Multiplied(persistenceId.toInt, amount))
            .thenRun { newState =>
              ctx.log.info(s"The state result is ${newState.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"Receive dividing for number: $amount and state is ${state.value}")
          Effect
            .persist(Divided(persistenceId.toInt, amount))
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.divide(amount)
      }
  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {
    initDataBase

    implicit val session = SlickSession.forConfig("slick-postgres")
    //system.registerOnTermination(session.close())

    implicit val materializer            = system.classicSystem
    materializer.registerOnTermination(() => session.close())
    var (offset, latestCalculatedResult) = getLatestOffsetAndResult
    val startOffset: Int                 = if (offset == 1) 1 else offset + 1

    def getEventName(event: EventEnvelope): String = {
      val parts = event.event.getClass.toString.split('$')
      parts(parts.length - 1)
    }

    val beforeLogFlow = Flow[EventEnvelope].map { x =>
      val eventName = getEventName(x)
      println(s"!Before Log from $eventName: $latestCalculatedResult")
      x
    }

    val processEventAmount = Flow[EventEnvelope].map { x =>
      x.event match {
        case Added(_, amount) => latestCalculatedResult += amount
        case Multiplied(_, amount) => latestCalculatedResult *= amount
        case Divided(_, amount) => latestCalculatedResult /= amount
      }
      x
    }

    val updateDb = Flow[EventEnvelope].map { x =>
      updateResultAndOfsset(latestCalculatedResult, x.sequenceNr)
      x
    }

    val updateDbSlick = Flow[EventEnvelope].map { x =>
      val query = sqlu"update public.result set calculated_value = $latestCalculatedResult, write_side_offset = ${x.sequenceNr} where id = 1"
      val result = Await.result (session.db.run(query), Duration.Inf)
      x
    }

    val afterLogFlow = Flow[EventEnvelope].map { x =>
      val eventName = getEventName(x)
      println(s"! Log from $eventName: $latestCalculatedResult")
      x
    }


//    val readJournal: LeveldbReadJournal =
    val readJournal: CassandraReadJournal =
      PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] = readJournal
      .eventsByPersistenceId("001", startOffset, Long.MaxValue)

    val printEvent = Flow[EventEnvelope].map { x =>
      println(x.toString())
      x
    }

    source
      .via(printEvent)
      .via(beforeLogFlow)
      .via(processEventAmount)
      .via(updateDbSlick)
      .via(afterLogFlow)
      .runWith(Sink.ignore)

  }


  object CalculatorRepository {
    import scalikejdbc._

    def initDataBase: Unit = {
      Class.forName("org.postgresql.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)

      ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
    }

    def getLatestOffsetAndResult: (Int, Double) = {
      val entities =
        DB readOnly { session =>
          session.list("select * from public.result where id = 1;") {
            row => (row.int("write_side_offset"), row.double("calculated_value")) }
        }
      entities.head
    }

    def updateResultAndOfsset(calculated: Double, offset: Long): Unit = {
      using(DB(ConnectionPool.borrow())) { db =>
        db.autoClose(true)
        db.localTx {
          _.update("update public.result set calculated_value = ?, write_side_offset = ? where id = ?", calculated, offset, 1)
        }
      }
    }
  }


  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calculato", Props.empty)

      writeActorRef ! Add(10)
      writeActorRef ! Multiply(2)
      writeActorRef ! Divide(5)
/*
      (1 to 1000).foreach{ x =>
        writeActorRef ! Add(10)
      }
*/


      // 0 + 10 = 10
      // 10 * 2 = 20
      // 20 / 5 = 4

      Behaviors.same
    }


  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")

    TypedCalculatorReadSide(system)

    implicit val executionContext = system.executionContext
  }
}