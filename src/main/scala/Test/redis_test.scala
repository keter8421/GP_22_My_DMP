package Test

import redis.clients.jedis.Jedis

object redis_test {
  def main(args: Array[String]): Unit = {
    val redis = new Jedis("yuke",6379)

    redis.select(2)

    redis.set("key1","str")

    redis.close()
  }
}
