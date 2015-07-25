package io.seldon.naivebayes;

import org.apache.log4j.Logger;
import org.apache.spark.mllib.classification.NaiveBayesModel;

import com.fasterxml.jackson.databind.ObjectMapper;

public class NaiveBayesStore {
	
	private static Logger logger = Logger.getLogger(NaiveBayesStore.class);

	private NaiveBayesModel model;
	private NaiveBayesData naiveBayesData;
	
	public static NaiveBayesStore createNaiveBayesStore(String data){
		
		try {
			ObjectMapper mapper = new ObjectMapper();
			NaiveBayesData naiveBayesData = mapper.readValue(data, NaiveBayesData.class);
			NaiveBayesStore naiveBayesStore = new NaiveBayesStore(naiveBayesData);
			return naiveBayesStore;
	    } catch (Throwable t) {
	        logger.error(null,t);
	    }
		return null;
	}

	public NaiveBayesStore(NaiveBayesData data){
		logger.info("NaiveBayesStore construct...");
		this.naiveBayesData = data;
		this.model = new NaiveBayesModel(naiveBayesData.modelData.labels,
				naiveBayesData.modelData.pi, naiveBayesData.modelData.theta);
		logger.info("NaiveBayesStore construct completed!");
	}

	public String[] getAttributeNames(){
		return naiveBayesData.attributeNames;
	}
	
	public NaiveBayesModel getModel(){
		return this.model;
	}

}
