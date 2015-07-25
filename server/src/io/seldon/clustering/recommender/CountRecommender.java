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

package io.seldon.clustering.recommender;

import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.recommendation.RecommendationUtils;
import io.seldon.util.CollectionTools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class CountRecommender {

	private static Logger logger = Logger.getLogger(CountRecommender.class.getName());
	
	UserClusterStore userClusters;
	ClusterCountStore clusterCounts;
	IClusterFromReferrer clusterFromReferrer;
	String client;
	boolean fillInZerosWithMostPopular = true;
	String referrer;
	
	private static int EXPIRE_COUNTS = 300;
	private static int EXPIRE_USER_CLUSTERS = 600;
	public static final int BUCKET_CLUSTER_ID = -1;
	
	
	public CountRecommender(String client,UserClusterStore userClusters,ClusterCountStore clusterCounts,IClusterFromReferrer clusterFromReferrer) {
		this.userClusters = userClusters;
		this.clusterCounts = clusterCounts;
		this.client = client;
		this.clusterFromReferrer = clusterFromReferrer;
	}
	
	
	
	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	
	
	private Set<Integer> getReferrerClusters()
	{
		if (referrer != null)
		{
			if (clusterFromReferrer != null)
				return clusterFromReferrer.getClusters(referrer);
			else
				return null;
		}
		else
			return null;
	}

	
	/**
	 * 
	 * @param userId
	 * @param itemId
	 * @param time - in secs
	 */
	public void addCount(long userId,long itemId,long time, boolean useBucketCluster,Double actionWeight)
	{
		if (actionWeight == null) actionWeight = 1.0D;
		List<UserCluster> clusters = getClusters(userId,null);
		if (clusters != null && clusters.size()>0)
		{
			for(UserCluster cluster : clusters)
				clusterCounts.add(cluster.getCluster(), itemId,cluster.getWeight() * actionWeight,cluster.getTimeStamp(),time);
		} else if(useBucketCluster) {
            clusterCounts.add(BUCKET_CLUSTER_ID, itemId, actionWeight, 0,time);
        }

		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters != null)
		{
			for (Integer cluster : referrerClusters)
			{
				clusterCounts.add(cluster, itemId,actionWeight,0,time);
			}
		}
	}
	
	public void addCount(long userId,long itemId, boolean useBucketCluster,Double actionWeight)
	{
		if (actionWeight == null) actionWeight = 1.0D;
		List<UserCluster> clusters = getClusters(userId,null);
		if (clusters != null && clusters.size() > 0)
		{
			for(UserCluster cluster : clusters)
				clusterCounts.add(cluster.getCluster(), itemId,cluster.getWeight()*actionWeight,cluster.getTimeStamp());
		} else if(useBucketCluster){
            clusterCounts.add(BUCKET_CLUSTER_ID, itemId, actionWeight, 0);
        }
		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters != null)
		{
			for (Integer cluster : referrerClusters)
			{
				clusterCounts.add(cluster, itemId, actionWeight, 0);
			}
		}
	}
	
	public Map<Long,Double> recommendUsingTag(Map<String,Float> tagWeights,int tagAttrId,Set<Integer> dimensions,Integer dimension2,int numRecommendations,Set<Long> exclusions,double decay,int minNumItems)
	{
		boolean checkDimension = !(dimensions.isEmpty() || (dimensions.size()==1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION));
		int minAllowed = minNumItems < numRecommendations ? minNumItems : numRecommendations;
		Map<Long,Double> counts = new HashMap<>();
		int numTopCounts = numRecommendations * 2; // number of counts to get - defaults to twice the final number recommendations to return
		for(Map.Entry<String,Float> e : tagWeights.entrySet())
		{
			updateTagCounts(e.getKey(), e.getValue(), tagAttrId, dimensions, dimension2, checkDimension, numTopCounts, exclusions, counts, decay);
		}

		if (counts.keySet().size() < minAllowed)
		{
			if (logger.isDebugEnabled())
				logger.debug("Number of tag items found "+counts.keySet().size()+" is less than "+minAllowed+" so returning empty recommendation for cluster tag item recommender");
			return new HashMap<>();
		}
		else if (logger.isDebugEnabled())
			logger.debug("Number of tag items found "+counts.keySet().size());
		
		return RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
	}
	
	private void updateTagCounts(String tag,Float tagWeight,int tagAttrId,Set<Integer> dimensions,Integer dimension2,boolean checkDimension,int limit,Set<Long> exclusions,Map<Long,Double> counts,double decay)
	{
		Map<Long,Double> itemCounts = null;
		boolean localDimensionCheckNeeded = false;
		if (checkDimension)
		{
			try 
			{
				itemCounts = getClusterTopCountsForTagAndDimensions(tag, tagAttrId, dimensions, dimension2,limit, decay);
			} 
			catch (ClusterCountNoImplementationException e) 
			{
				localDimensionCheckNeeded = true;
			}
		}
		
		if (itemCounts == null)
		{
			try 
			{
				itemCounts = getClusterTopCountsForTag(tag, tagAttrId, limit, decay);
			}
			catch (ClusterCountNoImplementationException e) 
			{
				logger.error("Failed to get cluster counts as method not implemented",e);
				itemCounts = new HashMap<>();
			}
		}
		double maxCount = 0;
		for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			if (itemCount.getValue() > maxCount)
				maxCount = itemCount.getValue();
		if (maxCount > 0)
		{
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			{
				Long item = itemCount.getKey();
				if (checkDimension && localDimensionCheckNeeded)
				{ 
					Collection<Integer> dims = ItemService.getItemDimensions(new ConsumerBean(client),item);
					dims.retainAll(dimensions);
					if (dims.isEmpty())
						continue;
				}
				if (!exclusions.contains(item))
				{
					Double count = (itemCount.getValue()/maxCount) * tagWeight; 
					Double existing = counts.get(item);
					if (existing != null)
						count = count + existing;
					counts.put(item, count);
				}
			}
		}
	}
	
	public Map<Long,Double> recommendUsingItem(String recommenderType, long itemId,Set<Integer> dimensions,int numRecommendations,Set<Long> exclusions,double decay,String clusterAlg,int minNumItems)
	{
		boolean checkDimension = !(dimensions.isEmpty() || (dimensions.size()==1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION));
		int minAllowed = minNumItems < numRecommendations ? minNumItems : numRecommendations;
		if (logger.isDebugEnabled())
			logger.debug("Recommend using items - dimension "+StringUtils.join(dimensions, ",")+" num recomendations "+numRecommendations+" itemId "+itemId+" minAllowed:"+minAllowed+" client "+client);
		Map<Long,Double> res = new HashMap<>();
		if (clusterAlg != null)
		{
			//get the cluster id from the item if possibl
			List<UserCluster> clusters = new ArrayList<>();
			switch(clusterAlg)
			{
			case "NONE":
			case "LDA_USER":
				break;
			case "DIMENSION":
				//get dimension for item
				Collection<Integer> dims = ItemService.getItemDimensions(new ConsumerBean(client), itemId);
				if (dims != null)
					for(Integer d : dims)
						clusters.add(new UserCluster(0, d, 1.0D, 0, 0));
				break;
			case "LDA_ITEM":
				//get cluster from item_clusters table
				Integer dim = ItemService.getItemCluster(new ConsumerBean(client), itemId);
				if (dim != null)
					clusters.add(new UserCluster(0, dim, 1.0D, 0, 0));
				break;
			}
			
			if (clusters.size() > 0)
			{
				Map<Long,Double> counts = new HashMap<>();
				int numTopCounts = numRecommendations * 2; // number of counts to get - defaults to twice the final number recommendations to return
				for(UserCluster cluster : clusters)
				{
					updateCounts(recommenderType,0, cluster, dimensions, checkDimension, numTopCounts, exclusions, counts, 1.0D,decay);
				}
			

				if (counts.keySet().size() < minAllowed)
				{
					if (logger.isDebugEnabled())
						logger.debug("Number of items found "+counts.keySet().size()+" is less than "+minAllowed+" so returning empty recommendation for cluster item recommender for item "+itemId);
					return new HashMap<>();
				}
				
				res = RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
			}
			else if (logger.isDebugEnabled())
				logger.debug("No clusters for item "+itemId+" so returning empty results for item cluster count recommender");
		}
		return res;
	}
	

		
	private void updateCounts(String recommenderType,long userId,UserCluster cluster,Set<Integer> dimensions,boolean checkDimension,int numTopCounts,Set<Long> exclusions,Map<Long,Double> counts,double clusterWeight,double decay)
	{
		Map<Long,Double> itemCounts = null;
		boolean localDimensionCheckNeeded = false;
		if (checkDimension)
		{
			try 
			{
				itemCounts = getClusterTopCountsForDimension(recommenderType, cluster.getCluster(), dimensions,cluster.getTimeStamp(), numTopCounts,decay);
			} 
			catch (ClusterCountNoImplementationException e) 
			{
				localDimensionCheckNeeded = true;
			}
		}
		
		if (itemCounts == null)
		{
			try 
			{
				itemCounts = getClusterTopCounts(cluster.getCluster(),cluster.getTimeStamp(), numTopCounts,decay);
			}
			catch (ClusterCountNoImplementationException e) 
			{
				logger.error("Failed to get cluster counts as method not implemented",e);
				itemCounts = new HashMap<>();
			}
		}
		double maxCount = 0;
		for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			if (itemCount.getValue() > maxCount)
				maxCount = itemCount.getValue();
		if (maxCount > 0)
		{
			ItemPeer iPeer = null;
			if (localDimensionCheckNeeded)
				iPeer = Util.getItemPeer(client);
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			{
				Long item = itemCount.getKey();
				if (checkDimension && localDimensionCheckNeeded)
				{ 
					Collection<Integer> dims = ItemService.getItemDimensions(new ConsumerBean(client),item);
					dims.retainAll(dimensions);
					if (dims.isEmpty())
						continue;
				}
				if (!exclusions.contains(item))
				{
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * clusterWeight; // weight cluster count by user weight for cluster and long term weight
					if (logger.isDebugEnabled())
						logger.debug("Adding long term count "+count+" for item "+item+" for user "+userId);
					Double existing = counts.get(item);
					if (existing != null)
						count = count + existing;
					counts.put(item, count);
				}
				else if (logger.isDebugEnabled())
					logger.debug("Ignoring excluded long term item in cluster recommendation "+item+" for user "+userId);
			}
		}
	}
	
	public Map<Long,Double> recommendGlobal(Set<Integer> dimensions,int numRecommendations,Set<Long> exclusions,double decay,Integer dimension2)
	{
		boolean checkDimension = !(dimensions.isEmpty() || (dimensions.size()==1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION));
		int numTopCounts = numRecommendations * 5;
		Map<Long,Double> itemCounts = null;
		boolean localDimensionCheckNeeded = false;
		if (checkDimension)
		{
			try 
			{
				itemCounts = getClusterTopCountsForDimension(dimensions,numTopCounts,decay,dimension2);
			} 
			catch (ClusterCountNoImplementationException e) 
			{
				localDimensionCheckNeeded = true;
			}
		}
		
		if (itemCounts == null)
		{
			try 
			{
				itemCounts = getClusterTopCounts(numTopCounts,decay);
				if (itemCounts == null)
				{
					logger.warn("Got null itemcounts");
					itemCounts = new HashMap<>();
				}
			}
			catch (ClusterCountNoImplementationException e) 
			{
				logger.error("Failed to get cluster counts as method not implemented",e);
				itemCounts = new HashMap<>();
			}
		}
		int excluded = 0;
		for(Iterator<Map.Entry<Long, Double>> i = itemCounts.entrySet().iterator();i.hasNext();)
		{
			Map.Entry<Long, Double> e = i.next();
			Long item = e.getKey();
			if (checkDimension && localDimensionCheckNeeded)
			{ 
				Collection<Integer> dims = ItemService.getItemDimensions(new ConsumerBean(client),item);
				dims.retainAll(dimensions);
				if (dims.isEmpty())
				{
					i.remove();
					excluded++;
				}
			}
			else if (exclusions.contains(item))
			{
				i.remove();
				excluded++;
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("Recommend global for dimension "+StringUtils.join(dimensions, ",")+" numRecs "+numRecommendations+" decay "+decay+" #in map "+itemCounts.size()+" excluded "+excluded+ " client "+client);
		return RecommendationUtils.rescaleScoresToOne(itemCounts, numRecommendations);
	}
	
	/**
	 * Provide a set of recommendations for a user using the counts from their clusters
	 * @param userId
	 * @param group
	 * @param numRecommendations
	 * @param includeShortTermClusters
	 * @param longTermWeight
	 * @param shortTermWeight
	 * @return
	 */
	public Map<Long,Double> recommend(String recommenderType,long userId,Integer group,Set<Integer> dimensions,int numRecommendations,Set<Long> exclusions,boolean includeShortTermClusters,double longTermWeight,double shortTermWeight,double decay,int minNumItems)
	{
		boolean checkDimension = !(dimensions.isEmpty() || (dimensions.size()==1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION));		
		int minAllowed = minNumItems < numRecommendations ? minNumItems : numRecommendations;
		if (logger.isDebugEnabled())
			logger.debug("Recommend for user clusters - dimension "+StringUtils.join(dimensions, ",")+" num recomendations "+numRecommendations+"minAllowed:"+minAllowed+ " client "+client+" user "+userId);

		// get user clusters pruned by group
		List<UserCluster> clusters;
		List<UserCluster> shortTermClusters;
		if (userId == Constants.ANONYMOUS_USER)
		{
			clusters = new ArrayList<>();
			shortTermClusters = new ArrayList<>();
		}	
		else
		{
			clusters = getClusters(userId,group);
			if (includeShortTermClusters)
				shortTermClusters = getShortTermClusters(userId, group);
			else
				shortTermClusters = new ArrayList<>();
		}
		// fail early
		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters == null || referrerClusters.size() == 0)
		{
			if (!includeShortTermClusters && clusters.size() == 0)
			{
				logger.debug("User has no long term clusters and we are not including short term clusters - so returning empty recommendations");
				return new HashMap<>();
			}
			else if (includeShortTermClusters && clusters.size() == 0 && shortTermClusters.size() == 0)
			{
				logger.debug("User has no long or short term clusters - so returning empty recommendations");
				return new HashMap<>();
			}
		}
		List<Long> res = null;
		Map<Long,Double> counts = new HashMap<>();
		int numTopCounts = numRecommendations * 5; // number of counts to get - defaults to twice the final number recommendations to return
		if (logger.isDebugEnabled())
			logger.debug("recommending using long term cluster weight of "+longTermWeight+" and short term cluster weight "+shortTermWeight+" decay "+decay);
		for(UserCluster cluster : clusters)
		{
			updateCounts(recommenderType,userId, cluster, dimensions, checkDimension, numTopCounts, exclusions, counts, longTermWeight,decay);
		}
		for(UserCluster cluster : shortTermClusters)
		{
			updateCounts(recommenderType, userId, cluster, dimensions, checkDimension, numTopCounts, exclusions, counts, shortTermWeight,decay);
		}

		if (referrerClusters != null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Adding "+referrerClusters.size()+" referrer clusters to counts for user "+userId+" client "+client);
			for(Integer c : referrerClusters)
			{
				UserCluster uc = new UserCluster(userId, c, 1.0, 0, 0);
				updateCounts(recommenderType,userId, uc, dimensions, checkDimension, numTopCounts, exclusions, counts, longTermWeight,decay);
			}
		}
		
		if (counts.keySet().size() < minAllowed)
		{
			if (logger.isDebugEnabled())
				logger.debug("Number of items found "+counts.keySet().size()+" is less than "+minAllowed+" so returning empty recommendation for user "+userId+" client "+client);
			return new HashMap<>();
		}
		
		return RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
	}	
	
	public List<Long> sort(long userId,List<Long> items,Integer group)
	{
		return this.sort(userId, items, group, false, 1.0D, 1.0D);
	}
	
	
	public List<Long> sort(long userId,List<Long> items,Integer group,boolean includeShortTermClusters,double longTermWeight,double shortTermWeight)
	{
		// get user clusters pruned by group
		List<UserCluster> clusters = getClusters(userId,group);
		List<UserCluster> shortTermClusters;
		if (includeShortTermClusters)
			shortTermClusters = getShortTermClusters(userId, group);
		else
			shortTermClusters = new ArrayList<>();
		if (!includeShortTermClusters && clusters.size() == 0)
		{
			logger.debug("User has no long term clusters and we are not including short term clusters - so returning empty recommendations");
			return new ArrayList<>();
		}
		else if (includeShortTermClusters && clusters.size() == 0 && shortTermClusters.size() == 0)
		{
			logger.debug("User has no long or short term clusters - so returning empty recommendations");
			return new ArrayList<>();
		}
		List<Long> res = null;
		Map<Long,Double> counts = new HashMap<>();
		for(long item : items) // initialise counts to zero
			counts.put(item, 0D);
		for(UserCluster cluster : clusters)
		{
			Map<Long,Double> itemCounts = getClusterCounts(cluster.getCluster(),cluster.getTimeStamp(),items);
			double maxCount = 0;
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				if (itemCount.getValue() > maxCount)
					maxCount = itemCount.getValue();
			if (maxCount > 0)
				for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				{
					Long item = itemCount.getKey();
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * longTermWeight; // weight cluster count by user weight for cluster and long term weight
					if (logger.isDebugEnabled())
						logger.debug("Adding long term count "+count+" for item "+item+" for user "+userId);
					counts.put(item, counts.get(item) + count);
				}
		}
		for(UserCluster cluster : shortTermClusters)
		{
			Map<Long,Double> itemCounts = getClusterCounts(cluster.getCluster(),cluster.getTimeStamp(),items);
			double maxCount = 0;
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				if (itemCount.getValue() > maxCount)
					maxCount = itemCount.getValue();
			if (maxCount > 0)
				for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				{
					Long item = itemCount.getKey();
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * shortTermWeight; // weight cluster count by user weight for cluster and short term weight
					if (logger.isDebugEnabled())
						logger.debug("Adding short term count "+count+" for item "+item+" for user "+userId);
					counts.put(item, counts.get(item) + count);
				}
		}
		if (this.fillInZerosWithMostPopular)
		{
			res = new ArrayList<>();
			List<Long> cRes = CollectionTools.sortMapAndLimitToList(counts, items.size()); 
			for(Long item : cRes)
				if (counts.get(item) > 0)
					res.add(item);
				else
					break;
		}
		else
			res = CollectionTools.sortMapAndLimitToList(counts, items.size());
		
		return res;
	}
	
	private List<UserCluster> getShortTermClusters(long userId,Integer group)
	{
		//Get Dynamic short-term clusters
		List<UserCluster> clusters = (List<UserCluster>) MemCachePeer.get(MemCacheKeys.getShortTermClustersForUser(client, userId));
		if (clusters != null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Got "+clusters.size()+" short term clusters for user "+userId);
		}
		else
		{
			if (logger.isDebugEnabled())
				logger.debug("Got 0 short term clusters for user "+userId);
			clusters = new ArrayList<>();
		}
		return clusters;
	}
	
	private List<UserCluster> getClusters(long userId,Integer group)
	{
		List<UserCluster> clusters = null;
		String memcacheKey = null;
		if (userClusters.needsExternalCaching())
		{
			memcacheKey = MemCacheKeys.getClustersForUser(client,userId);
			clusters = (List<UserCluster>) MemCachePeer.get(memcacheKey);
		}
		if (clusters == null)
		{
			clusters = userClusters.getClusters(userId);
			if (userClusters.needsExternalCaching())
				MemCachePeer.put(memcacheKey, clusters, EXPIRE_USER_CLUSTERS);
		}
		
		
		//prune clusters not in desired group
		if (group != null)
		{
			for(Iterator<UserCluster> i=clusters.iterator();i.hasNext();)
			{
				if (!group.equals(i.next().getGroup()))
					i.remove();
			}
		}
		return clusters;
	}
	
	private Map<Long,Double> getClusterTopCountsForDimension(final Set<Integer> dimensions, final int limit, final double decay, final Integer dimension2) throws ClusterCountNoImplementationException {
		if (dimension2 != null)
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForTwoDimensions(client, dimensions, dimension2, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							if (logger.isDebugEnabled())
								logger.debug("Trying to get top counts for dimension from db : for client " + client + " dimension:" + StringUtils.join(dimensions, ",") + " dimension2:" + dimension2);
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByTwoDimensions(dimensions, dimension2, limit, decay);
							return new ClustersCounts(itemMap, 0);
						}
					}
			);
		else {
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimension(client, dimensions, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							if (logger.isDebugEnabled())
								logger.debug("Trying to get top counts for dimension from db : testMode is for client " + client + " dimension:" + StringUtils.join(dimensions, ","));
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByDimension(dimensions, limit, decay);

							return new ClustersCounts(itemMap, 0);
						}
					}
			);

		}
	}
	
	private Map<Long,Double> getClusterTopCountsForTag(final String tag,final int tagAttrId, final int limit, final double decay) throws ClusterCountNoImplementationException {
		
		return getClusterCounts(MemCacheKeys.getTopClusterCountsForTag(client, tag, tagAttrId, limit),
				new UpdateRetriever<ClustersCounts>() {
					@Override
					public ClustersCounts retrieve() throws Exception {
						if (logger.isDebugEnabled())
							logger.debug("Trying to get top counts for tag and dimension from db : testMode is for client " + client);
						Map<Long, Double> itemMap = clusterCounts.getTopCountsByTag(tag, tagAttrId, limit, decay);

						return new ClustersCounts(itemMap, 0);
					}
				}
		);
}
	
	private Map<Long,Double> getClusterTopCountsForTagAndDimensions(final String tag,final int tagAttrId,final Set<Integer> dimensions, final Integer dimension2, final int limit, final double decay) throws ClusterCountNoImplementationException {
		
		if (dimension2 != null)
		{
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForTagAndTwoDimensions(client, tag, tagAttrId, dimensions, dimension2, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							if (logger.isDebugEnabled())
								logger.debug("Trying to get top counts for tag and two dimensions from db : testMode is for client " + client + " dimension1:" + StringUtils.join(dimensions, ",")+ " dimension2:"+dimension2);
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByTagAndTwoDimensions(tag, tagAttrId, dimensions, dimension2, limit, decay);

							return new ClustersCounts(itemMap, 0);
						}
					}
			);
		}
		else
		{
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForTagAndDimension(client, tag, tagAttrId, dimensions, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							if (logger.isDebugEnabled())
								logger.debug("Trying to get top counts for tag and dimension from db : testMode is for client " + client + " dimension:" + StringUtils.join(dimensions, ","));
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByTagAndDimension(tag, tagAttrId, dimensions, limit, decay);

							return new ClustersCounts(itemMap, 0);
						}
					}
			);
			
		}
	}

	private Map<Long, Double> getClusterCounts(String memcacheKey,UpdateRetriever<ClustersCounts> retriever) throws ClusterCountNoImplementationException {
		if(clusterCounts.needsExternalCaching()){
			ClustersCounts itemCounts = (ClustersCounts) MemCachePeer.get(memcacheKey);
			ClustersCounts newItemCounts = null;
			try {
				newItemCounts = DogpileHandler.get().retrieveUpdateIfRequired(memcacheKey, itemCounts, retriever, EXPIRE_COUNTS);
			} catch (Exception e){
				if (e instanceof ClusterCountNoImplementationException){
					throw (ClusterCountNoImplementationException) e;
				}else{
					logger.error("Unknown exception:" , e);
				}
			}
			if(newItemCounts !=null ){
				MemCachePeer.put(memcacheKey, newItemCounts , EXPIRE_COUNTS);
				return newItemCounts.getItemCounts();
			} else {
				if(itemCounts==null){
					logger.warn("Couldn't get cluster counts from store or memcache. Returning null");
					return new HashMap<Long,Double>();
				} else {
					return itemCounts.getItemCounts();
				}
			}
		} else {
			try {
				return retriever.retrieve().getItemCounts();
			} catch (Exception e) {
				if (e instanceof ClusterCountNoImplementationException){
					throw (ClusterCountNoImplementationException) e;
				}else{
					logger.error("Unknown exception:" , e);
					return new HashMap<Long,Double>();
				}
			}
		}
	}
	
	private Map<Long,Double> getClusterTopCountsForDimension(String recommenderType,final int clusterId,final Set<Integer> dimensions,final long timestamp,final int limit,final double decay) throws ClusterCountNoImplementationException
	{
			switch(recommenderType)
			{
			case "CLUSTER_COUNTS_SIGNIFICANT":
				return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimensionAlg(client, recommenderType, clusterId, dimensions, limit),
						new UpdateRetriever<ClustersCounts>() {
							@Override
							public ClustersCounts retrieve() throws Exception{
								if (logger.isDebugEnabled())
									logger.debug("Trying to get top counts from db : testMode is for client " + client + " cluster id:" + clusterId + " dimension:" + StringUtils.join(dimensions, ",") + " limit:" + limit);
								Map<Long, Double> itemMap = clusterCounts.getTopSignificantCountsByDimension(clusterId, dimensions, timestamp, limit, decay);
								return new ClustersCounts(itemMap, timestamp);
							}
						}
				);
			default:
				return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimension(client, clusterId, dimensions, limit),
						new UpdateRetriever<ClustersCounts>() {
							@Override
							public ClustersCounts retrieve() throws Exception {
								if (logger.isDebugEnabled())
									logger.debug("Trying to get top counts from db : testMode is for client " + client + " cluster id:" + clusterId + " dimension:" + StringUtils.join(dimensions, ",") + " limit:" + limit);
								Map<Long, Double> itemMap = clusterCounts.getTopCountsByDimension(clusterId, dimensions, timestamp, limit, decay);
								return new ClustersCounts(itemMap, timestamp);
							}
						}
				);
			}

	}

	private Map<Long,Double> getClusterTopCounts(final int limit,final double decay) throws ClusterCountNoImplementationException
	{
		return getClusterCounts(MemCacheKeys.getTopClusterCounts(client,limit),new UpdateRetriever<ClustersCounts>() {
			@Override
			public ClustersCounts retrieve() throws Exception {
				Map<Long,Double> itemMap = clusterCounts.getTopCounts(limit, decay);
				return new ClustersCounts(itemMap,0);
			}
		});
	}
	
	private Map<Long,Double> getClusterTopCounts(final int clusterId, final long timestamp, final int limit, final double decay) throws ClusterCountNoImplementationException
	{
		return getClusterCounts(MemCacheKeys.getTopClusterCounts(client, clusterId, limit), new UpdateRetriever<ClustersCounts>() {
			@Override
			public ClustersCounts retrieve() throws Exception {
				Map<Long,Double> itemMap = clusterCounts.getTopCounts(clusterId, timestamp, limit, decay);
				return new ClustersCounts(itemMap,timestamp);
			}
		});

	}
	
	private Map<Long,Double> getClusterCounts(final int clusterId, final long timestamp, final List<Long> items)
	{

		try {
			return getClusterCounts(MemCacheKeys.getClusterCountForItems(client, clusterId, items, timestamp), new UpdateRetriever<ClustersCounts>() {
                @Override
                public ClustersCounts retrieve() throws Exception {
                    Map<Long,Double> itemMap = new HashMap<>();
                    for(Long itemId : items)
                    {
                        double count = clusterCounts.getCount(clusterId, itemId, timestamp);
                        itemMap.put(itemId, count);
                    }
                    return new ClustersCounts(itemMap,timestamp);
                }
            });
		} catch (ClusterCountNoImplementationException e) {
			// can't happen
			return null;
		}

	}
	
	
	
}
