package io.seldon.naivebayes;

import io.seldon.api.APIException;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.UserAttributePeer;
import io.seldon.general.UserPeer;
import io.seldon.general.jdo.SqlUserAttributePeer;
import io.seldon.general.jdo.SqlUserPeer;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.linalg.DenseVector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jdo.PersistenceManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class NaiveBayesRecommender implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(NaiveBayesRecommender.class);
	private static final String name = NaiveBayesRecommender.class.getSimpleName();

	@Autowired
	private NaiveBayesManager naiveBayesManager;

	private PersistenceManager getPersistenceManager(String client) {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(client);
		if (pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return pm;
	}

	private UserPeer getUserPeer(String client) {
		PersistenceManager pm = getPersistenceManager(client);
		return new SqlUserPeer(pm);
	}

	private UserAttributePeer getUserAttributePeer(String client) {
		PersistenceManager pm = getPersistenceManager(client);
		return new SqlUserAttributePeer(pm);
	}

	@Override
	public ItemRecommendationResultSet recommend(String clientId, Long userId,
	                                             Set<Integer> dimensions, int maxRecsCount,
	                                             RecommendationContext ctxt, List<Long> recentItemInteractions) {
		UserAttributePeer userAttributePeer = getUserAttributePeer(clientId);

		Map<String, String> userAttributesNameMap =  userAttributePeer.getUserAttributesName(userId);

		for(String key : userAttributesNameMap.keySet()){
			String value = userAttributesNameMap.get(key);
			logger.info(String.format("userAttributesNameMap: %s=%s",
					key, value));
		}

		NaiveBayesStore store = naiveBayesManager.getClientStore(clientId);

		String[] storeAttributeNames = store.getAttributeNames();

		double[] userAttributeValueArray = new double[storeAttributeNames.length];
		for (int index = 0; index < userAttributeValueArray.length; index++) {
			String attributeName = storeAttributeNames[index];
			Double attributeValue = Double.parseDouble(userAttributesNameMap.get(attributeName));
			if(attributeValue == null) attributeValue = 0d;
			userAttributeValueArray[index] = attributeValue;
		}

		StringBuilder sb = new StringBuilder(1024);
		for (int index = 0; index < userAttributeValueArray.length; index++) {
			if (index > 0) {
				sb.append(",");
			}
			sb.append(String.format("%f", userAttributeValueArray[index]));
		}
		logger.debug(String.format("UserAttributeValueArray: (%s)", sb.toString()));

		NaiveBayesStore clientStore = naiveBayesManager.getClientStore(clientId);
		DenseVector userVector = new DenseVector(userAttributeValueArray);
		long itemId = Math.round(clientStore.getModel().predict(userVector));
		logger.info(String.format("ItemId: %d", itemId));

		List<ItemRecommendationResultSet.ItemRecommendationResult> recommendations = new ArrayList<>();
		recommendations.add(new ItemRecommendationResultSet.ItemRecommendationResult(itemId, 1f));
		return new ItemRecommendationResultSet(recommendations, name);
	}

	@Override
	public String name() {
		return name;
	}

}
