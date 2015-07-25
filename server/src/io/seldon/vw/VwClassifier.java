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
package io.seldon.vw;

import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionResult;
import io.seldon.prediction.PredictionsResult;
import io.seldon.vw.VwFeatureExtractor.Namespace;
import io.seldon.vw.VwModelManager.VwModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class VwClassifier implements PredictionAlgorithm {
	private static Logger logger = Logger.getLogger(VwClassifier.class.getName());
	VwModelManager modelManager;
	VwFeatureExtractor featureExtractor;
	
	@Autowired
    public VwClassifier(VwModelManager modelManager,VwFeatureExtractor featureExtractor)
    {
		this.modelManager = modelManager;
		this.featureExtractor = featureExtractor;
    }
	
	private double sigmoid(double val)
	{
		return 1.0/(1.0 + Math.exp(-val));
	}
	
	private List<PredictionResult> normalise(List<PredictionResult> scores)
	{
		double sum = 0.0;
		for(PredictionResult p : scores)
			sum = sum + p.confidence;
		for(PredictionResult p : scores)
			p.confidence = p.confidence / sum;
		return scores;
	}
	
	@Override
	public PredictionsResult predict(String client, JsonNode jsonNode,OptionsHolder options) {

		VwModel model = modelManager.getClientStore(client,options);
		if (model == null)
		{
			logger.warn("No model found for client"+client);
			return new PredictionsResult();
		}
		else
		{
			List<Namespace> namespaces = featureExtractor.extract(jsonNode);
			List<PredictionResult> predictions = new ArrayList<PredictionResult>();
			for(int i=0;i<model.oaa;i++)
			{
				float score = 0;
				for(Namespace n : namespaces)
				{
					for(Map.Entry<String, Float> e : n.features.entrySet())
					{
						Integer index = model.hasher.getFeatureHash(i+1, n.name, e.getKey());
						Float weight = model.weights.get(index);
						if (weight != null)
							score = score + (e.getValue() * weight);
					}
				}
				Integer constantIndex = model.hasher.getConstantHash(i+1);
				Float weight = model.weights.get(constantIndex);
				if (weight != null)
					score = score + weight;
				String classId = ""+(i+1);
				if (model.classIdMap.containsKey(i+1))
					classId = model.classIdMap.get(i+1);
				predictions.add(new PredictionResult((double)score, classId, sigmoid(score)));
			}
			//aribrary decision point at 0.0 for binary classification
			if (model.oaa == 1 && predictions.get(0).prediction < 0)
				if (model.classIdMap.containsKey(-1))
					predictions.get(0).predictedClass = model.classIdMap.get(-1);
				else
					predictions.get(0).predictedClass = "-1";
					
			
			return new PredictionsResult(normalise(predictions));
		}
	}

}
