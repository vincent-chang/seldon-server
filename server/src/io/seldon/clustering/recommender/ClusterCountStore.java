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

import java.util.Map;
import java.util.Set;

public interface ClusterCountStore {

	public void setAlpha(double alpha); // time decay parameter
	public void add(int clusterId,long itemId,double weight,long clusterTimestamp);
	public void add(int clusterId,long itemId,double weight,long clusterTimestamp,long time);
	public double getCount(int clusterId,long itemId,long timestamp);
	
	/*
	 * Global methods
	 */
	//Global top counts
	public Map<Long,Double> getTopCounts(int limit, double decay) throws ClusterCountNoImplementationException;
	//Global top counts but restricted to a dimension for returned items
	public Map<Long,Double> getTopCountsByDimension(Set<Integer> dimensions,int limit, double decay) throws ClusterCountNoImplementationException;
	//Global top counts but restricted to two dimensions for returned items
	public Map<Long,Double> getTopCountsByTwoDimensions(Set<Integer> dimension1,int dimension2,int limit, double decay) throws ClusterCountNoImplementationException;

	/*
	 * Tag menthods
	 */
	//Global counts but using a tag from a textual attribute plus general dimension
	public Map<Long,Double> getTopCountsByTagAndDimension(String tag,int tagAttrId,Set<Integer> dimensions,int limit,double decay) throws ClusterCountNoImplementationException;
	public Map<Long,Double> getTopCountsByTagAndTwoDimensions(String tag,int tagAttrId,Set<Integer> dimensions,int dimension2,int limit,double decay) throws ClusterCountNoImplementationException;
	public Map<Long,Double> getTopCountsByTag(String tag,int tagAttrId,int limit,double decay) throws ClusterCountNoImplementationException;
	/*
	 * Methods restricted to a cluster
	 */
	//top counts within a cluster
	public Map<Long,Double> getTopCounts(int clusterId,long timestamp,int limit, double decay) throws ClusterCountNoImplementationException;
	//top counts within a cluster restricted to a dimension
	public Map<Long,Double> getTopCountsByDimension(int clusterId,Set<Integer> dimensions,long timestamp,int limit, double decay) throws ClusterCountNoImplementationException;
	//top significant items within a cluster restricted to a dimension
	public Map<Long,Double> getTopSignificantCountsByDimension(int clusterId,Set<Integer> dimensions,long timestamp,int limit, double decay) throws ClusterCountNoImplementationException;	



	public boolean needsExternalCaching();
	
}
