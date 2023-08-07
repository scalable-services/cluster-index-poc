package cluster.helpers

import cluster.{KEYSPACE, loader}
import cluster.grpc._
import cluster.grpc.tests.ListIndex
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index.{AsyncIndexIterator, Serializer, Storage, Tuple}
import services.scalable.index.grpc.{IndexContext, KVPair}

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps

object TestHelper {

  def saveListIndex[K, V](id: String, data: Seq[(K, V, String)], session: CqlSession,
                          kser: Serializer[K], vser: Serializer[V]): Boolean = {
    val listIndex = ListIndex(id, data.map { case (k, v, _) =>
      KVPair().withKey(ByteString.copyFrom(kser.serialize(k)))
        .withValue(ByteString.copyFrom(vser.serialize(v)))
    })

    val serializedListIndex = Any.pack(listIndex).toByteArray

    val ustm = session.prepare(s"INSERT INTO history.test_indexes(id, data) values (?, ?);")
    val bind = ustm.bind(id, ByteBuffer.wrap(serializedListIndex))

    //bind.setString(0, id)
    //bind.setByteBuffer("data", ByteBuffer.wrap(serializedListIndex))

    session.execute(bind).wasApplied()
  }

  def loadListIndex(id: String, session: CqlSession): Option[ListIndex] = {
    val ustm = session.prepare(s"select * from test_indexes where id = ?;")
    val bind = ustm.bind(id)

    session.execute(bind).all().asScala.map { r =>
      Any.parseFrom(r.getByteBuffer("data").flip().array()).unpack(ListIndex)
    }.headOption
  }

  def createRange(range: RangeIndexMeta)(implicit session: CqlSession, ec: ExecutionContext): Future[Boolean] = {
    val stm = session.prepare("INSERT INTO ranges(id, data) VALUES(?, ?);")
      .bind(range.id, ByteBuffer.wrap(Any.pack(range).toByteArray))

    session.executeAsync(stm).toCompletableFuture.asScala.map { r =>
      r.wasApplied()
    }
  }

  def getRange(id: String)(implicit session: CqlSession, ec: ExecutionContext): Future[Option[RangeIndexMeta]] = {
    val stm = session.prepare("SELECT * from ranges where id = ?;")
      .bind(id)

    session.executeAsync(stm).toCompletableFuture.asScala.map { r =>
      val one = r.one()
      if(one == null) None else Some(Any.parseFrom(one.getByteBuffer("data").array()).unpack(RangeIndexMeta))
    }
  }

  def saveRange(range: RangeIndexMeta)(implicit session: CqlSession, ec: ExecutionContext): Future[Boolean] = {
    val stm = session.prepare("UPDATE ranges set data = ? where id = ?;")
      .bind(ByteBuffer.wrap(Any.pack(range).toByteArray), range.id)

    session.executeAsync(stm).toCompletableFuture.asScala.map { r =>
      r.wasApplied()
    }
  }

  def truncateAll()(implicit session: CqlSession, ec: ExecutionContext): Unit = {
    println("truncate ranges: ", session.execute("TRUNCATE TABLE ranges;").wasApplied())
    println("truncate indexes: ", session.execute("TRUNCATE TABLE indexes;").wasApplied())
    println("truncate indexes: ", session.execute("TRUNCATE TABLE blocks;").wasApplied())
  }

  def loadOrCreateIndex(tctx: IndexContext)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(tctx.id).flatMap {
      case None => storage.createIndex(tctx).map(_ => Some(tctx))
      case Some(t) => Future.successful(Some(t))
    }
  }

  def loadIndex(id: String)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(id).flatMap {
      case None => Future.successful(None)
      case Some(t) => Future.successful(Some(t))
    }
  }

  def all[K, V](it: AsyncIndexIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def getSession(): CqlSession = {
    CqlSession
      .builder()
      //.withLocalDatacenter("datacenter1")
      .withConfigLoader(loader)
      .withKeyspace(KEYSPACE)
      //.withAuthCredentials(CQL_USER, CQL_PWD)
      .build()
  }

}
