package cluster.helpers

object TestConfig {

  val CLUSTER_INDEX_NAME = "index-1"
  val NUM_LEAF_ENTRIES = 8
  val NUM_META_ENTRIES = 8
  val MAX_RANGE_ITEMS = 100

  val META_INDEX_TOPIC = "meta"
  val RANGE_INDEX_TOPIC = "range"
  val RESPONSE_TOPIC = "responses"
  val N_PARTITIONS = 3

  val TX_VERSION = "v1"
}
