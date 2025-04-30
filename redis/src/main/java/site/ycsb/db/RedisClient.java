/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;
  private String host;
  private String redisTimeout;
  private String portString;
  private int port;
  // private JedisCommands jedis1;
  // private JedisCommands jedis2;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String HOST2_PROPERTY= "redis.host2";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";


  private static final int MAX_RETRIES = 200;
  private static final long RETRY_DELAY_MS = 50;

  @FunctionalInterface
  private interface RedisCommand<T> {
    T run(JedisCommands jedis) throws Exception;
  }

  private <T> T runWithReconnect(RedisCommand<T> command) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        return command.run(jedis);
      } catch (Exception e) {
        System.err.println("[" + LocalDateTime.now().format(formatter) +
            "] Redis operation failed: " + e + ". Retrying " + (i + 1) + "/" + MAX_RETRIES);
        try {
          long delay = RETRY_DELAY_MS * (i + 1);
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted", ie);
        }

        if (jedis instanceof Jedis) {
          try {
            reconnect();
            System.out.println("[" + LocalDateTime.now().format(formatter) + "] Redis connection Successful");
          } catch (Exception ie) {
            System.err.println("[" + LocalDateTime.now().format(formatter) + "] Redis reconnect failed: " + ie);
          }
        }
      }
    }
    throw new RuntimeException("Redis Command Failed after max retries reached");
  }

  public void reconnect() throws DBException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    System.out.println("Reconnecting to Redis server...");
    cleanup();
    if (redisTimeout != null){
      System.out.println("[" + LocalDateTime.now().format(formatter) + "] init jedis with timeout: " + redisTimeout);
      jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
    } else {
      System.out.println("[" + LocalDateTime.now().format(formatter) + "] init jedis with: " + host+":"+portString);
      jedis = new Jedis(host, port);
    }
    ((Jedis) jedis).connect();
  }

  // private<T> T runWithReconnect(RedisCommand<T> command) {
  //   for (int i=0; i<MAX_RETRIES; i++){
  //     try {
  //       return command.run(jedis);
  //     } catch(Exception e) {
  //       System.err.println("Redis operation failed: " + e + ". Retrying" + (i+1) + "/" + MAX_RETRIES);
  //       try {
  //         Thread.sleep(RETRY_DELAY_MS);
  //       } catch (InterruptedException ie) {
  //         Thread.currentThread().interrupt();
  //         throw new RuntimeException("Thread interrupted", ie);
  //       }
  //
  //       // Attemp reconnect only if using raw Jedis
  //       if (jedis instanceof Jedis) {
  //         try {
  //           ((Jedis) jedis).close();
  //           ((Jedis) jedis).connect();
  //         } catch (Exception ie) {
  //           System.err.println("Redis reconnect failed: " + ie);
  //         }
  //       } else {
  //         // For JedisCluster, we don't need to reconnect
  //         // as it handles reconnections internally.
  //       }
  //     }
  //   }
  //   throw new RuntimeException("Redis Command Failed after max retries reached");
  // }


  public void init() throws DBException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    Properties props = getProperties();

    portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    host = props.getProperty(HOST_PROPERTY);
    System.out.println("[" + LocalDateTime.now().format(formatter) + "] Redis host: " + host);

    String host2 = props.getProperty(HOST2_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null){
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] init jedis with timeout: " + redisTimeout);
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
        // jedis2 = new Jedis(host2, port, Integer.parseInt(redisTimeout));
      } else {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] init jedis with: " + host+":"+portString);
        jedis = new Jedis(host, port);
        // jedis2 = new Jedis(host2, port);
      }
      // jedis = jedis1;
      ((Jedis) jedis).connect();
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      // StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
      Map<String, String> map = runWithReconnect(j -> j.hgetAll(key));
      StringByteIterator.putAllAsByteIterators(result, map);
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      // List<String> values = jedis.hmget(key, fieldArray);
      List<String> values = runWithReconnect(j -> j.hmget(key, fieldArray));

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    // if (jedis.hmset(key, StringByteIterator.getStringMap(values))
    //     .equals("OK")) {
    //   jedis.zadd(INDEX_KEY, hash(key), key);
    //   return Status.OK;
    // }
    String response = runWithReconnect(j->j.hmset(key, StringByteIterator.getStringMap(values)));
    if ("OK".equals(response)) {
      runWithReconnect(j -> {
          j.zadd(INDEX_KEY, hash(key), key);
          return null;
        });
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {

    Long delResult = runWithReconnect(j->j.del(key));
    Long zremResult = runWithReconnect(j->j.zrem(INDEX_KEY, key));

    return delResult == 0 && zremResult == 0 ? Status.ERROR : Status.OK;
    // return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
    //     : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    String response = runWithReconnect(j->j.hmset(key, StringByteIterator.getStringMap(values)));
    if ("OK".equals(response)) {
      return Status.OK;
    }
    return Status.ERROR;
    // return jedis.hmset(key, StringByteIterator.getStringMap(values))
    //     .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
    //     Double.POSITIVE_INFINITY, 0, recordcount);

    Set<String> keys = runWithReconnect(j->j.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount));

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
