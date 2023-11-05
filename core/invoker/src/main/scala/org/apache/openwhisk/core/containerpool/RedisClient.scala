/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import java.time.Instant
import org.apache.openwhisk.common._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


class RedisClient(
  host: String = "172.17.0.1", 
  port: Int = 6379, 
  password: String = "openwhisk", 
  database: Int = 0,
  logging: Logging
) {
  private var pool: JedisPool = _
  val interval: Int = 1000 //ms

  def init: Unit = {
    val maxTotal: Int = 300
    val maxIdle: Int = 100
    val minIdle: Int = 1
    val timeout: Int = 30000

    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    pool = new JedisPool(poolConfig, host, port, timeout, password, database)
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }

  //
  // Send observations to Redis
  //

  def setWastedMemoryTime(containerId: String, wastedMemoryTime: String): Unit = {
    try {
      val jedis = pool.getResource
      val name: String = "wasted_memory_time"
      val key: String = containerId
      val value: String = wastedMemoryTime
      jedis.hset(name, key, value)
      jedis.close()
    } catch {
      case e: Exception => {
        logging.error(this, s"set wasted memory time error, exception ${e}, at ${Instant.now.toEpochMilli}")
      }
    }
  }
}
