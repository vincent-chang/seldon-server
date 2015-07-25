package io.seldon.naivebayes;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Component
public class NaiveBayesManager implements PerClientExternalLocationListener{

	private static Logger logger = Logger.getLogger(NaiveBayesManager.class);
	private final ConcurrentMap<String,NaiveBayesStore> clientStores = new ConcurrentHashMap<>();
	private NewResourceNotifier notifier;
	private final ExternalResourceStreamer featuresFileHandler;
	public static final String NB_NEW_LOC_PATTERN = "naivebayes";
	private final Executor executor = Executors.newFixedThreadPool(5);
	
	@Autowired
	public NaiveBayesManager(ExternalResourceStreamer featuresFileHandler,
                             NewResourceNotifier notifier){
		logger.info(String.format(
				"Construct NaiveBayesManager with: featuresFileHandler=%s, notifier=%s",
				featuresFileHandler, notifier));
		this.featuresFileHandler = featuresFileHandler;
        this.notifier = notifier;
        this.notifier.addListener(NB_NEW_LOC_PATTERN, this);
    }
	
	@PostConstruct
    public void init(){
		
    }
	
	public void reloadFeatures(final String location, final String client){
        executor.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Reloading naive bayes features for client: "+ client);
	            try {
		            InputStream modelDataInputStream =
				            featuresFileHandler.getResourceStream(location + "/NaiveBayesModel.json");
		            InputStreamReader modelDataInputStreamReader =
				            new InputStreamReader(modelDataInputStream);
		            BufferedReader modelDataBufferedReader = new BufferedReader(modelDataInputStreamReader);
		            String modelData = modelDataBufferedReader.readLine();
		            logger.info("Naive bayes model data load completed!");
		            if(StringUtils.isNotEmpty(modelData)) {
			            logger.info("Create naive bayes model...");
			            NaiveBayesStore store = NaiveBayesStore.createNaiveBayesStore(modelData);
			            logger.info("Naive bayes model created!");
			            clientStores.put(client, store);
		            }else{
			            logger.warn("Naive bayes features is empty!");
		            }
		            modelDataBufferedReader.close();
	            } catch (FileNotFoundException e) {
		            logger.error("Couldn't reloadFeatures for client "+ client, e);
	            } catch (IOException e) {
		            logger.error("Couldn't reloadFeatures for client "+ client, e);
	            }
            }
        });
    }
	
	public NaiveBayesStore getClientStore(String client){
        return clientStores.get(client);
    }

	@Override
	public void newClientLocation(String client, String configValue,
			String configKey) {
		reloadFeatures(configValue,client);
	}

	@Override
	public void clientLocationDeleted(String client, String nodePattern) {
		clientStores.remove(client);
	}

}
