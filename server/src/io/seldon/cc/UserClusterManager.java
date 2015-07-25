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

package io.seldon.cc;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.clustering.recommender.MemoryUserClusterStore;
import io.seldon.clustering.recommender.UserCluster;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;


@Component
public class UserClusterManager implements PerClientExternalLocationListener {

	 private static Logger logger = Logger.getLogger(UserClusterManager.class.getName());
	 private final ConcurrentMap<String,MemoryUserClusterStore> clientStores = new ConcurrentHashMap<>();
	 private final ConcurrentMap<String,ClusterDescription> clusterDescriptions = new ConcurrentHashMap<>();
	 private Set<NewResourceNotifier> notifiers = new HashSet<>();
	 private final ExternalResourceStreamer featuresFileHandler;
	 public static final String CLUSTER_NEW_LOC_PATTERN = "userclusters";

	 private static UserClusterManager theManager; // hack until rest of code Springified
	 
	 //private final Executor executor = Executors.newFixedThreadPool(5);
	 private BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();
	 private ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 5, 10, TimeUnit.MINUTES, queue) {
	        protected void afterExecute(java.lang.Runnable runnable, java.lang.Throwable throwable)
	        {
	        	        	JDOFactory.get().cleanupPM();
	        }
	    };

	 private ItemService itemService;
	 
	 @Autowired
	 public UserClusterManager(ExternalResourceStreamer featuresFileHandler,NewResourceNotifier notifier,ItemService itemService){
	        this.featuresFileHandler = featuresFileHandler;
	        notifiers.add(notifier);
	        notifier.addListener(CLUSTER_NEW_LOC_PATTERN, this);
	        this.theManager = this;
	        this.itemService = itemService;
	 }
	 
	 public static UserClusterManager get()
	 {
		 return theManager;
	 }
	 
	 public void reloadFeatures(final String location, final String client){
	        executor.execute(new Runnable() {
	            @Override
	            public void run() {
	                logger.info("Reloading user clusters for client: "+ client);

	                try {
	                    BufferedReader reader = new BufferedReader(new InputStreamReader(
	                            featuresFileHandler.getResourceStream(location + "/part-00000")
	                    ));

	                    MemoryUserClusterStore userClusters = loadUserClusters(client, reader);
	                    clientStores.put(client, userClusters);
	                    reader.close();

	                    logger.info("finished load of user clusters for client "+client);
	                }
	                catch (FileNotFoundException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                } catch (IOException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                }


	            }
	        });

	    }

	 protected MemoryUserClusterStore loadUserClusters(String client,BufferedReader reader) throws IOException
	 {
		 String line;
		 List<UserCluster> clusters = new ArrayList<>();
		 ObjectMapper mapper = new ObjectMapper();
		 int numUsers = 0;
		 int numClusters = 0;
		 long lastUser = -1;
		 Set<Integer> dimensions = new HashSet<Integer>();
		 while((line = reader.readLine()) !=null)
		 {
			 UserDimWeight data = mapper.readValue(line.getBytes(), UserDimWeight.class);
			 if (lastUser != data.user)
				 numUsers++;
			 clusters.add(new UserCluster(data.user, data.dim, data.weight, 0, 0));
			 dimensions.add(data.dim);
			 lastUser = data.user;
			 numClusters++;
		 }
		 MemoryUserClusterStore store = new MemoryUserClusterStore(client,numUsers);
		 storeClusters(store,clusters);
		 store.setLoaded(true);
		 setClusterDescription(client, dimensions);
		 logger.info("Loaded user clusters for client "+client+" with num users "+numUsers+" and number of clusters "+numClusters);
		 return store;
	 }
	 
	 private void setClusterDescription(String client,Set<Integer> dimensions)
	 {
		 ConsumerBean c = new ConsumerBean(client);
		 Map<Integer,String> clusterNames = new HashMap<>();
		 try
		 {
			 for(Integer dim : dimensions)
			 {
				 String[] names = itemService.getDimensionName(c, dim);
				 if (names != null && names.length == 2)
				 {
					 clusterNames.put(dim, names[0]+":"+names[1]);
				 }
				 else
					 logger.warn("Can't find cluster name in db for dimension "+dim+" for "+client);
			 }
		 }
		 catch (Exception e)
		 {
			 logger.error("Failed to create cluster descriptions for "+client,e);
		 }
		 clusterDescriptions.put(client, new ClusterDescription(clusterNames));
	 }
	 
	 private void storeClusters(MemoryUserClusterStore store,List<UserCluster> clusters)
		{
			long currentUser = -1;
			List<UserCluster> userClusters = new ArrayList<>();
			for(UserCluster cluster : clusters)
			{
				if (currentUser != -1 && currentUser != cluster.getUser())
				{
					store.store(currentUser, userClusters);
					userClusters = new ArrayList<>();
				}
				userClusters.add(cluster);
				currentUser = cluster.getUser();
			}
			if (userClusters.size() > 0)
				store.store(currentUser, userClusters);
		}
	 
	public MemoryUserClusterStore getStore(String client)
	{
		return clientStores.get(client);
	}

	public ClusterDescription getClusterDescriptions(String client)
	{
		return clusterDescriptions.get(client);
	}
	
	@Override
	public void newClientLocation(String client, String location,
			String nodePattern) {
		reloadFeatures(location, client);
	}

	@Override
	public void clientLocationDeleted(String client, String nodePattern) {
		clientStores.remove(client);
	}

	public static class ClusterDescription {
		
		public final Map<Integer,String> clusterNames;

		public ClusterDescription(Map<Integer, String> clusterNames) {
			super();
			this.clusterNames = clusterNames;
		}
		
		
	}
	
}
