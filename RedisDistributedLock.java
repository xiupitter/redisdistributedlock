package simpleDistLockTest;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 基于redis的分布式锁
 * 
 * @author Xiupitter
 *
 */
public class RedisDistributedLock implements Lock{

  private static JedisPool pool;
	private String key;
	private long timeout;

	static{
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(30);
        poolConfig.setMinIdle(30);
        poolConfig.setMinEvictableIdleTimeMillis(500);
        poolConfig.setTestOnBorrow(true);
        pool = new JedisPool(poolConfig, "192.168.192.128", 6379);
	}
	
	public static JedisPool getJedisPool(){
		return pool;
	}
	
	/**
	 * @param key 分布式锁必须基于一个key，该key就是竞争资源，本地锁lock本身就是竞争资源，远程锁必须有个key表示锁的本体是在远程的，并且锁的竞争
	 * 资源就是该key，一般为string这样所有分布式节点都可以简单的通过该string获得锁
	 * @param lockTimeout 锁过期时间，如果一个进程没有unlock，则其他人无法进入锁，所以需要一个过期时间
	 */
	public RedisDistributedLock(String key,long lockTimeout){
		this.key =key;
		this.timeout =lockTimeout;
	}

	@Override
	public synchronized void lock() {
		// TODO Auto-generated method stub
		Jedis j = RedisDistributedLock.getJedisPool().getResource();
		while(j.setnx("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1))!=1){
			if((Long.parseLong(j.get("lock."+key))<System.currentTimeMillis()+1)&&//检测是否超时
					(Long.parseLong(j.getSet("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1)))<System.currentTimeMillis()+1)){
					break;
			}
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		RedisDistributedLock.getJedisPool().returnResource(j);
	}

	@Override
	public synchronized void lockInterruptibly() throws InterruptedException {
		// TODO Auto-generated method stub
		Jedis j = RedisDistributedLock.getJedisPool().getResource();
		while(j.setnx("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1))!=1){
			if(Long.parseLong(j.get("lock."+key))<System.currentTimeMillis()+1&&
					(Long.parseLong(j.getSet("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1)))<System.currentTimeMillis()+1)){
					break;
			}
			Thread.sleep(20);
		}
		RedisDistributedLock.getJedisPool().returnResource(j);
	}

	@Override
	public synchronized boolean tryLock() {
		// TODO Auto-generated method stub
		Jedis j = RedisDistributedLock.getJedisPool().getResource();
		if(j.setnx("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1))!=1){
			if(Long.parseLong(j.get("lock."+key))<System.currentTimeMillis()+1&&
					(Long.parseLong(j.getSet("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1)))<System.currentTimeMillis()+1)){
				RedisDistributedLock.getJedisPool().returnResource(j);
				return true;
			}
			RedisDistributedLock.getJedisPool().returnResource(j);
			return false;
		}else{
			RedisDistributedLock.getJedisPool().returnResource(j);
			return true;
		}
	}

	@Override
	public synchronized boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		// TODO Auto-generated method stub
		long timeAccumulate = 0;
		Jedis j = RedisDistributedLock.getJedisPool().getResource();
		while(j.setnx("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1))!=1){
			
			if(Long.parseLong(j.get("lock."+key))<System.currentTimeMillis()+1&&
					(Long.parseLong(j.getSet("lock."+key, String.valueOf(System.currentTimeMillis()+timeout+1)))<System.currentTimeMillis()+1)){//过期锁
				RedisDistributedLock.getJedisPool().returnResource(j);
				return true;
			}
			if(timeAccumulate>unit.toNanos(time)){
				RedisDistributedLock.getJedisPool().returnResource(j);
				return false;
			}
			Thread.sleep(20);
			timeAccumulate+=20*1000000;
		}
		RedisDistributedLock.getJedisPool().returnResource(j);
		return true;
	}

	@Override
	public synchronized void unlock() {
		// TODO Auto-generated method stub
		Jedis j = RedisDistributedLock.getJedisPool().getResource();
		if(System.currentTimeMillis()<Long.parseLong(j.get("lock."+key)))//it is not timeout,so
			j.del("lock."+key);
		RedisDistributedLock.getJedisPool().returnResource(j);
	}

	@Override
	public synchronized Condition newCondition() {
		// TODO Auto-generated method stub
		return null ;
	}
	
}
