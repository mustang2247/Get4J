package com.bytegriffin.get4j.store;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bytegriffin.get4j.util.Queue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import redis.clients.jedis.JedisCommands;

public class RedisQueue<E> implements Queue<E> {

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

	private final Lock readLock = readWriteLock.readLock();

	private final Lock writeLock = readWriteLock.writeLock();

	private  LinkedList<E> list = Lists.newLinkedList();
	// key：redis_key  value：score count
	private final Map<String, Integer> score_map = Maps.newHashMap();

	private JedisCommands jedis;

	public RedisQueue( JedisCommands jedisCommand) {
		this.jedis = jedisCommand;
	}

	@Override
	public void add(E e) {
        writeLock.lock();
        try {
            if (!contains(e)) {
                list.add(e);
            }
        } finally {
            writeLock.unlock();
        }
    }

	@Override
	public void add(String key, E e) {
		writeLock.lock();
		try {
			if (!contains(e)) {
				int count = 0;
				if(score_map.containsKey(key)){
					count = score_map.get(key) + 1;
				}
				jedis.zadd(key, count, (String) e);
				score_map.put(key, count);
			}
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public E get(int index) {
		readLock.lock();
		try {
			return list.get(index);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public long size() {
		readLock.lock();
		try {
			return list.size();
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public long size(String queueName) {
		readLock.lock();
		try {
			return jedis.zcard(queueName);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public void clear() {
		writeLock.lock();
		try {
			list.clear();
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void clear(String queueName) {
		writeLock.lock();
		try {
			list.clear();
			jedis.zremrangeByRank(queueName, 0, -1);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		writeLock.lock();
		try {
			return list.isEmpty();
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean isEmpty(String queueName) {
		writeLock.lock();
		try {
			return !jedis.exists(queueName);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public E outFirst() {
		writeLock.lock();
		try {
			if (!list.isEmpty()) {
				return list.removeFirst();
			}
			return null;
		} finally {
			writeLock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public E outFirst(String queueName) {
		writeLock.lock();
		try {
				Set<String> url = jedis.zrange(queueName, 0, 0);
				long count = 0;
				if(url != null && url.size() > 0){
					count = jedis.zremrangeByRank(queueName, 0, 0);
				}
				if(count == 1){
					return (E) url.iterator().next();
				}
				return null;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean contains(E e) {
		return list.contains(e);
	}

	@Override
	public boolean contains(String queueName, E e) {
		Long count = jedis.zrank(queueName, (String) e);
		if(count != null && count > 0) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Queue<E> getQueue(String queueName) {
		Set<String> set = jedis.zrange(queueName, 0, -1);
		RedisQueue<E> queue = new RedisQueue<E>(jedis);
		queue.list.clear();
		for(String str : set){
			queue.list.add((E) str);
		}
		return queue;
	}

	/**
	 * 不能单独调用此方法
	 */
	@Override
	public LinkedList<E> getList() {
		return list;
	}

}
