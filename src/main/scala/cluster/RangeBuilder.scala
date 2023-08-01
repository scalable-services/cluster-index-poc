package cluster

import com.datastax.oss.driver.api.core.CqlSession
import services.scalable.index.Serializer

import scala.concurrent.ExecutionContext

case class RangeBuilder[K, V](val ORDER: Int = cluster.ORDER)(implicit val ordering: Ordering[K],
                           val session: CqlSession,
                           val ec: ExecutionContext,
                           val ks: Serializer[K],
                           val vs: Serializer[V],
                           val kts: K => String,
                           val vts: V => String){
  val MIN: Int = ORDER / 2
  val MAX: Int = ORDER
}
