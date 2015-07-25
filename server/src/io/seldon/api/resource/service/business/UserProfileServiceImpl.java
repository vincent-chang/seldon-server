/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.api.resource.service.business;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.ScoreBean;
import io.seldon.api.resource.UserProfileBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.api.state.ClientAlgorithmStore;
import io.seldon.cc.UserClusterManager;
import io.seldon.cc.UserClusterManager.ClusterDescription;
import io.seldon.clustering.recommender.MemoryUserClusterStore;
import io.seldon.clustering.recommender.UserCluster;
import io.seldon.tags.UserTagAffinityManager;
import io.seldon.tags.UserTagAffinityManager.UserTagStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserProfileServiceImpl implements UserProfileService {
	private static Logger logger = Logger.getLogger(UserProfileServiceImpl.class.getName());

	private static final String USER_TAG_MODEL = "tags";
	private static final String CLUSTER_MODEL = "cluster";
	private static final String ALL_MODELS = USER_TAG_MODEL+","+CLUSTER_MODEL;
	
	@Autowired
    private UserService userService;
	
	@Autowired
	private UserTagAffinityManager tagAffinityManager;
	
	@Autowired
	private UserClusterManager userClusterManager;

	@Autowired
	private ClientAlgorithmStore clientAlgorithmStore;
	
	@Override
	public ResourceBean getProfile(ConsumerBean consumerBean, String userId,
			String models) {

		ListBean res = new ListBean();
		ArrayList<ResourceBean> profiles = new ArrayList<>();
		res.setList(profiles);
		
		long intUserId;
		try 
		{ 
			intUserId = userService.getInternalUserId(consumerBean, userId);	
		}
		catch(Exception e) {
			logger.debug("Not possibile to get profile for user with no internal id");
			return res;
		 }
		
		if (models == null)
			models = ALL_MODELS;
		
		int modelsLoaded = 0;
		for(String model : models.split(","))
		{
			switch(model)
			{
				case USER_TAG_MODEL:
				{
					List<ScoreBean> scores = getUserTagAffinities(consumerBean, intUserId);
					if (scores != null)
					{
						profiles.add(new UserProfileBean(userId, USER_TAG_MODEL, scores));
						modelsLoaded++;
					}
				}
				break;
				case CLUSTER_MODEL:
				{
					List<ScoreBean> scores = getClusterWeights(consumerBean, intUserId);
					if (scores != null)
					{
						profiles.add(new UserProfileBean(userId, CLUSTER_MODEL, scores));
						modelsLoaded++;
					}
				}
				break;
			}
		}
		res.setSize(modelsLoaded);
		return res;
	}
	
	private List<ScoreBean> getClusterWeights(ConsumerBean c,long user)
	{
		MemoryUserClusterStore clusterStore = userClusterManager.getStore(c.getShort_name());
		if (clusterStore != null)
		{
			List<ScoreBean> clusterWeights = new ArrayList<ScoreBean>();
			List<UserCluster> clusters = clusterStore.getClusters(user);
			if (clusters != null)
			{
				ClusterDescription clusterDescr = userClusterManager.getClusterDescriptions(c.getShort_name());
				for(UserCluster cluster : clusters)
				{
					String name;
					if (clusterDescr != null && clusterDescr.clusterNames.containsKey(cluster.getCluster()))
						name = clusterDescr.clusterNames.get(cluster.getCluster());
					else
						name = ""+cluster.getCluster();
					clusterWeights.add(new ScoreBean(name, cluster.getWeight()));
				}	
			}
			return clusterWeights;
		}
		else
			return null;
	}
	
	private List<ScoreBean> getUserTagAffinities(ConsumerBean c,long user)
	{

		UserTagStore userTagStore = tagAffinityManager.getStore(c.getShort_name());
		if (userTagStore == null)
			return null;
		else
		{
			Map<String,Float> tagAffinities = userTagStore.userTagAffinities.get(user);
			List<ScoreBean> tagScores = new ArrayList<ScoreBean>();
			if (tagAffinities != null)
				for(Map.Entry<String, Float> e : tagAffinities.entrySet())
					tagScores.add(new ScoreBean(e.getKey(), e.getValue()));
			return tagScores;
		}
	}

}
