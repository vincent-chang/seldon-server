package io.seldon.naivebayes;

import io.seldon.api.APIException;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.User;
import io.seldon.general.UserAttributePeer;
import io.seldon.general.UserAttributeValueVo;
import io.seldon.general.UserPeer;
import io.seldon.general.jdo.SqlUserAttributePeer;
import io.seldon.general.jdo.SqlUserPeer;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.linalg.DenseVector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jdo.PersistenceManager;
import java.util.*;

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
	public ItemRecommendationResultSet recommend(String client, Long user,
	                                             Set<Integer> dimensions, int maxRecsCount,
	                                             RecommendationContext ctxt, List<Long> recentItemInteractions) {
		UserPeer userPeer = getUserPeer(client);
		UserAttributePeer userAttributePeer = getUserAttributePeer(client);

		User userBean = userPeer.getUser(user);
		Map<Integer, UserAttributeValueVo> userAttributeValueVoMap
				= userAttributePeer.getAttributesForUser(user);

		Map<String, UserAttributeValueVo> userAttributeVoMap =
				new HashMap<>();

		for (Integer index : userAttributeValueVoMap.keySet()) {
			UserAttributeValueVo userAttributeValueVo = userAttributeValueVoMap.get(index);
			logger.info(String.format("Index: %d, UserAttributeValueVo: %s=%s",
					index, userAttributeValueVo.getName(), userAttributeValueVo.getValue()));
			userAttributeVoMap.put(userAttributeValueVo.getName(), userAttributeValueVo);
		}

		NaiveBayesStore store = naiveBayesManager.getClientStore(client);

		String[] storeAttributeNames = store.getAttributeNames();

		double[] userAttributeValueArray = new double[storeAttributeNames.length];
		for (int index = 0; index < userAttributeValueArray.length; index++) {
			UserAttributeValueVo userAttributeValueVo =
					userAttributeVoMap.get(storeAttributeNames[index]);
			double userAttributeValue = 0;
			if (userAttributeValueVo.getValue() == null) {
				userAttributeValue = 0;
			} else if (userAttributeValueVo.getValue() instanceof String) {
				userAttributeValue = Double.parseDouble((String) userAttributeValueVo.getValue());
			} else if (userAttributeValueVo.getValue() instanceof Double) {
				userAttributeValue = (Double) userAttributeValueVo.getValue();
			}
			userAttributeValueArray[index] = userAttributeValue;
		}

		StringBuilder sb = new StringBuilder(1024);
		for (int index = 0; index < userAttributeValueArray.length; index++) {
			if (index > 0) {
				sb.append(",");
			}
			sb.append(String.format("%f", userAttributeValueArray[index]));
		}
		logger.debug(String.format("UserAttributeValueArray: (%s)", sb.toString()));

		NaiveBayesStore clientStore = naiveBayesManager.getClientStore(client);
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
