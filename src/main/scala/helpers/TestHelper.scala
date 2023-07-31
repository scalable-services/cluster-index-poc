package helpers

import cluster.{KEYSPACE, loader}
import cluster.grpc._
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.any.Any

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

object TestHelper {

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

  def truncateRanges()(implicit session: CqlSession, ec: ExecutionContext): Future[Boolean] = {
    val stm = session.prepare("TRUNCATE TABLE ranges;").bind()

    session.executeAsync(stm).toCompletableFuture.asScala.map { r =>
      r.wasApplied()
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
