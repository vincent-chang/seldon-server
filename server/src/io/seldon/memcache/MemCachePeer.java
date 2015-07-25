/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.memcache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.log4j.Logger;

public class MemCachePeer {
	
	private static Logger logger = Logger.getLogger( MemCachePeer.class.getName() );
	private static final int MAX_CAS_RETRIES = 100;
//	 create a static client as most installs only need
	// a single instance
	protected static List<MemcachedClient> clients;
	protected static MemcachedClient theClient =  null;

    public static void initialise(String serverList,int  numClients) {
        try
        {
        	if (numClients > 1)
        	{
        		clients = new ArrayList<MemcachedClient>(numClients);
        		logger.info("Creating "+numClients+" memcache clients");
        		for(int i=0;i<numClients;i++)
        		{
        			ConnectionFactoryBuilder cb = new ConnectionFactoryBuilder(new DefaultConnectionFactory());
        			cb.setOpTimeout(ExceptionSwallowingMemcachedClient.MEMCACHE_OP_TIMEOUT);
        			clients.add(new MemcachedClient(cb.build(),AddrUtil.getAddresses(serverList)));
        		}
        	}
        	else
        	{
        		logger.info("Creating single memcache client");
    			ConnectionFactoryBuilder cb = new ConnectionFactoryBuilder(new DefaultConnectionFactory());
    			cb.setOpTimeout(ExceptionSwallowingMemcachedClient.MEMCACHE_OP_TIMEOUT);
    			theClient = new MemcachedClient(cb.build(),AddrUtil.getAddresses(serverList));
        	}

        	logger.info(String.format("MemcachedClient initialised using serverList[%s]",serverList+" with "+numClients+" clients"));
        }
        catch (IOException e)
        {
            logger.error("Can't create memcache connection ",e);
        }
    }
    
    /*
     * Get random memcache client
     */
    private static MemcachedClient getClient()
    {
    	if (theClient != null)
    		return theClient;
    	else
    		return clients.get(ThreadLocalRandom.current().nextInt(clients.size()));
    }

	public static void delete(String key)
	{
		MemcachedClient client = getClient();
		if (client != null)
		try
		{
			client.delete(hashKey(key));
		}
		catch (Exception ex)
		{
			logger.warn("Memcache delete exeption ",ex);
		}
	}
	
	public static void put(String key,Object obj)
	{
		MemcachedClient client = getClient();
		if (client != null)
		try
		{
			client.set(hashKey(key), 0, obj);
		}
		catch (Exception ex)
		{
			logger.warn("Memcache put exeption ",ex);
		}
	}
	
	/*
	 *  Expire in seconds
	 */
	public static void put(String key,Object obj,int expireSeconds)
	{
		MemcachedClient client = getClient();
		if (client != null)
			try
			{
				client.set(hashKey(key), expireSeconds, obj);
			}
			catch (Exception ex)
			{
				logger.error("Memcache put expire exeption ",ex);
			}
	}
	
	public static Object get(String key)
	{
		MemcachedClient client = getClient();
		Object myObj=null;
		if (client != null)
		{
			Future<Object> f=client.asyncGet(hashKey(key));
			try 
			{
			    myObj=f.get(ExceptionSwallowingMemcachedClient.MEMCACHE_OP_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch(TimeoutException e) {
				logger.error("Timeout exception in get ",e);
			    f.cancel(false);
			} catch (InterruptedException e) {
				logger.error("Interrupted in get ",e);
				f.cancel(false);
			} catch (ExecutionException e) {
				logger.error("Execution exception in get ",e);
				f.cancel(false);
			}
		}
	    return myObj;
	}
	
	public static CASValue gets(String key)
	{
		MemcachedClient client = getClient();
		if (client != null)
		{
		try
		{
			return client.gets(hashKey(key));
		}
		catch (Exception ex)
		{
			logger.warn("Memcache get exeption ",ex);
			return null;
		}
		}
		else
			return null;
	}
	
	public static <T> T cas(String key,CASMutation<T> mutation,T value)
	{
		return cas(key,mutation,value,0);
	}
	
	/**
	 * Method to allow CAS 
	 * @param <T>
	 * @param key
	 * @param mutation
	 * @param value
	 * @return
	 */
	public static <T> T cas(String key,CASMutation<T> mutation,T value,int expireSecs)
	{
		MemcachedClient client = getClient();
		 if (client != null)
		 {
			 Transcoder transcoder = new SerializingTranscoder();
			 // The mutator who'll do all the low-level stuff.
			 // Set number of retries to limit time taken..its not essential this succeeds
			 CASMutator<T> mutator = new CASMutator<>(client, transcoder,MAX_CAS_RETRIES);

			 // This returns whatever value was successfully stored within the
			 // cache -- either the initial list as above, or a mutated existing
			 // one
			 try 
			 {
				 return mutator.cas(hashKey(key), value, expireSecs, mutation);
			 } 
			 catch (Exception e) 
			 {
				 logger.error("Failed up update hits in cache ",e);
				 return null;
			 }
		 }
		 else
			 return null;
	}
	
	private static String hashKey(String key)
	{
		return SecurityHashPeer.md5digest(key);
	}
	
	

}
