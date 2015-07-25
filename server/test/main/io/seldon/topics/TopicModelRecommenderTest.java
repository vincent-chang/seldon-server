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

package io.seldon.topics;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.items.RecentItemsWithTagsManager;
import io.seldon.topics.TopicFeaturesManager.TopicFeaturesStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class TopicModelRecommenderTest {

	private TopicFeaturesManager mockFeaturesManager;
	private RecentItemsWithTagsManager mockTagsManager;
	private RecommendationContext mockCtxt;
	private RecommendationContext.OptionsHolder mockOptions;

	@Before
	public void createMocks()
	{
		mockFeaturesManager = createMock(TopicFeaturesManager.class);
		mockTagsManager = createMock(RecentItemsWithTagsManager.class);
		mockCtxt = createMock(RecommendationContext.class);
		mockOptions = createMock(RecommendationContext.OptionsHolder.class);
	}
	
	@Test
	public void testNoFeatures()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		expect(mockFeaturesManager.getClientStore(client, mockOptions)).andReturn(null);
		replay(mockFeaturesManager);
		TopicModelRecommender r = new TopicModelRecommender(mockFeaturesManager, mockTagsManager);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);

		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.attrid")).andReturn(0);
		expect(mockOptions.getStringOption("io.seldon.algorithm.tags.table")).andReturn("table");
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.minnumtagsfortopicweights")).andReturn(3);

		replay(mockCtxt, mockOptions);
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,50,mockCtxt,null);
		
		verify(mockFeaturesManager,mockCtxt, mockOptions);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(0, res.getResults().size());
		
	}
	
	@Test
	public void testNoTags()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		final int attrId = 1;
		final int limit = 10;
		final int numRecentItems = 1000;
		final String table = "varchar";
		Set<Long> recentItems = new HashSet<>();
		recentItems.add(1L);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);

//		expect(mockOptions.getIntegerOption("io.seldon.algorithm.general.numrecentactionstouse")).andReturn(numRecentItems);
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.attrid")).andReturn(attrId);
		expect(mockOptions.getStringOption("io.seldon.algorithm.tags.table")).andReturn(table);
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.minnumtagsfortopicweights")).andReturn(3);
		expect(mockTagsManager.retrieveRecentItems(EasyMock.eq(client), EasyMock.eq(recentItems), EasyMock.eq(attrId), EasyMock.eq(table))).andReturn(null);
		replay(mockTagsManager, mockOptions);

		TopicFeaturesStore tfs = new TopicFeaturesStore(Collections.<Long, Map<Integer,Float>>emptyMap(),Collections.<String, Map<Integer,Float>>emptyMap());
		expect(mockFeaturesManager.getClientStore(client,mockOptions)).andReturn(tfs);
		replay(mockFeaturesManager);
		expect(mockCtxt.getContextItems()).andReturn(Collections.singleton(1L)).times(3);
		replay(mockCtxt);
		TopicModelRecommender r = new TopicModelRecommender(mockFeaturesManager, mockTagsManager);

		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions, 50,mockCtxt,null);

		verify(mockFeaturesManager);
		verify(mockCtxt,mockTagsManager,mockOptions);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(0, res.getResults().size());

	}


	@Test
	public void testSimpleResults()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		final int attrId = 1;
		final int limit = 10;
		final int numRecentItems = 1000;
		final String table = "varchar";
		Set<Long> recentItems = new HashSet<>();


		Map<Long,List<String>> itemTags = new HashMap<>();
		final String tag = "tag";
		final Long itemId = 1L;
		List<String> tags = new ArrayList<>();
		tags.add(tag);
		itemTags.put(itemId, tags);
		recentItems.add(itemId);
		expect(mockTagsManager.retrieveRecentItems(EasyMock.eq(client), EasyMock.eq(recentItems), EasyMock.eq(attrId),EasyMock.eq(table))).andReturn(itemTags);
		replay(mockTagsManager);

		Map<Long,Map<Integer,Float>> userTopicWeights = new HashMap<>();
		Map<Integer,Float> topicWeights = new HashMap<>();
		final Long user = 1L;
		final Integer topic = 1;
		final Float topicWeight = 0.5f;
		topicWeights.put(topic, topicWeight);
		userTopicWeights.put(user, topicWeights);
		Map<String,Map<Integer,Float>> tagTopicWeights = new HashMap<>();
		final Float tagWeight = 0.5f;
		Map<Integer,Float> tagWeights = new HashMap<>();
		tagWeights.put(topic, tagWeight);
		tagTopicWeights.put(tag, tagWeights);
		TopicFeaturesStore tfs = new TopicFeaturesStore(userTopicWeights,tagTopicWeights);
		expect(mockFeaturesManager.getClientStore(client,mockOptions)).andReturn(tfs);
		replay(mockFeaturesManager);
		expect(mockCtxt.getContextItems()).andReturn(recentItems).times(3);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);
		replay(mockCtxt);


//		expect(mockOptions.getIntegerOption( "io.seldon.algorithm.general.numrecentactionstouse")).andReturn(numRecentItems);
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.attrid")).andReturn(attrId);
		expect(mockOptions.getStringOption("io.seldon.algorithm.tags.table")).andReturn(table);
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.tags.minnumtagsfortopicweights")).andReturn(0);
		replay(mockOptions);
		TopicModelRecommender r = new TopicModelRecommender(mockFeaturesManager, mockTagsManager);
		List<Long> recentItemInteractions = new ArrayList<Long>();
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,50, mockCtxt,recentItemInteractions);

		verify(mockCtxt, mockOptions);
		verify(mockFeaturesManager);
		verify(mockTagsManager);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(1, res.getResults().size());

	}
	
}
