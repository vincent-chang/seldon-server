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

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 11/12/14
 *         Time: 15:20
 */
@Component
public class ItemClusterCountsRecommender extends BaseItemClusterCountsRecommender implements ItemRecommendationAlgorithm {
	private static final String name = ItemClusterCountsRecommender.class.getSimpleName();
    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {
        return recommend( name, "CLUSTER_COUNTS_FOR_ITEM",client,user,dimensions,maxRecsCount,ctxt);
    }

    @Override
    public String name() {
        return name;
    }
}
