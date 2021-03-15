package metro

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory

import java.util.{Date, UUID}
import scala.collection.immutable
import scala.util.Random

case class TroykaCard(id: String, isAllowed: Boolean)
case class EntryAttempt(troykaCard: TroykaCard, date: Date)
case object EntryAccepted
case class EntryRejected(reason: String)

object Turnstile {
  def props(validator: ActorRef) = Props(new Turnstile(validator))
}

class Turnstile(validator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case o: TroykaCard =>
      log.info(s"$validator")
      validator ! EntryAttempt(o, new Date)
    case EntryAccepted => log.info("GREEN: пропуск оформлен, проходите пожалуйста")
    case EntryRejected(reason) => log.info(s"RED: $reason")
  }
}

class TroykaCovidPassValidator extends Actor with ActorLogging {
  override def preStart(): Unit = {
    super.preStart()
    log.info("Validator starting")
  }

  override def receive: Receive = {
    case EntryAttempt(card @ TroykaCard(_, isAllowed), _) =>
      log.info(s"Validating $card")
      if (isAllowed) sender() ! EntryAccepted
      else sender() ! EntryRejected(s"Не оформлен пропуск на mos.ru или карта не привязана")
  }
}

object TurnstileSettings {
  val numberOfShards = 3
  val numberOfEntities = 30

  /**
   * Создаем уникальный идентификатор ентинти
   * */
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case attempt @ EntryAttempt(TroykaCard(cardId, _), _) =>
      val entityId = cardId.hashCode.abs % numberOfEntities
      println(s"!!!!!!!!!!!!!! EXTRACT ENTITY ID for card # ${attempt.troykaCard.id} to entity ID ${entityId}")
      (entityId.toString, attempt)
  }

  /**
   * Cоздаем уникальный идентификатор группы (шарда) для каждого сообщения. Гарантия отсутствия дубликатов
   * */
  val extractShardId: ShardRegion.ExtractShardId = {
    case EntryAttempt(TroykaCard(cardId, _), _) =>
      val shardId = cardId.hashCode.abs % numberOfShards
      println(s"!!!!!!!!!!!!!! EXTRACT SHARD ID for card # ${cardId} to shard ID ${shardId}")
      shardId.toString
  }
}


/**
 * Обратите внимание, что MetroStation не запускает акторы TroykaCovidPassValidator, в отличие от singleton-версии Shoppers.
 * Модуль ClusterSharding автоматически запустит TroykaCovidPassValidator при попытке переслать ему команду.
 * Он извлечет идентификаторы shopperId и shardId из сообщения и создаст соответствующий экземпляр TroykaCovidPassValidator,
 * используя entryProps, переданный ранее. Все последующие команды будут передаваться этому актору TroykaCovidPassValidator.
 * */

class MetroStation(port: Int, amountOfTurnstiles: Int) extends App {

  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
     """.stripMargin)
    .withFallback(ConfigFactory.load("clusterShardingExample.conf"))

  val system = ActorSystem("DemoCluster", config)

  val validatorShardRegionRef: ActorRef = ClusterSharding(system).start(
    typeName = "TroykaCovidPassValidator",
    entityProps = Props[TroykaCovidPassValidator],
    settings = ClusterShardingSettings(system),
    extractEntityId = TurnstileSettings.extractEntityId,
    extractShardId = TurnstileSettings.extractShardId
  )

  val turnstiles: immutable.Seq[ActorRef] = (1 to amountOfTurnstiles)
    .map{x =>
      println(s"Before Starting actor of turnstiles #${x}")
      system.actorOf(Turnstile.props(validatorShardRegionRef))
    }

  Thread.sleep(10000)
  for (_ <- 1 to 1000) {
    val randomTurnstileIndex = Random.nextInt(amountOfTurnstiles)
    val randomTurnstile = turnstiles(randomTurnstileIndex)

    randomTurnstile ! TroykaCard(UUID.randomUUID().toString, Random.nextBoolean())
    Thread.sleep(200)
  }
}

object ChistyePrudy extends MetroStation(2551, 10)
object Lubyanka extends MetroStation(2561, 5)
object OkhotnyRyad extends MetroStation(2571, 15)