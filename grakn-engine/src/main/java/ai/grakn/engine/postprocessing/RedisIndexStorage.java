/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.controller.response.Keyspace;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 *     Stores a list of indices and vertex ids representing those indices which need to be post processed
 * </p>
 *
 * @author fppt
 */
public class RedisIndexStorage extends RedisStorage {
    private RedisIndexStorage(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        super(jedisPool, metricRegistry);
    }

    public static RedisIndexStorage create(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        return new RedisIndexStorage(jedisPool, metricRegistry);
    }

    /**
     * Add an index to the list of indices which needs to be post processed
     */
    public void addIndex(Keyspace keyspace, String index, Set<ConceptId> conceptIds){
        String listOfIndicesKey = getIndicesKey(keyspace);
        String listOfIdsKey = getConceptIdsKey(keyspace, index);

        contactRedis(jedis -> {
            //Track all the indices which need to be post proceed
            jedis.sadd(listOfIndicesKey, index);
            conceptIds.forEach(id -> jedis.sadd(listOfIdsKey, id.getValue()));
            return null;
        });
    }

    /**
     * Gets and removes the next index to post process
     */
    public String popIndex(Keyspace keyspace){
        String indexKey = getIndicesKey(keyspace);
        return contactRedis(jedis -> jedis.spop(indexKey));
    }

    /**
     * Gets and removes all the ids which we need to post process
     */
    public Set<ConceptId> popIds(Keyspace keyspace, String index){
        String idKey = getConceptIdsKey(keyspace, index);
        return contactRedis(jedis -> {
            Set<ConceptId> ids = jedis.smembers(idKey).stream().map(ConceptId::of).collect(Collectors.toSet());
            jedis.del(idKey);
            return ids;
        });
    }

    /**
     * The key which refers to  a list of all the indices in a certain {@link Keyspace} which need to be post processed
     */
    private static String getIndicesKey(Keyspace keyspace){
        return "IndicesToProcess_" + keyspace.value();
    }

    /**
     * The key which refers to a set of vertices currently pointing to the same index
     */
    private static String getConceptIdsKey(Keyspace keyspace, String index){
        return "IdsToPostProcess_" + keyspace.value() + "_Id_" + index;
    }
}