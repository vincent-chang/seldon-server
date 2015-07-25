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

package io.seldon.api.state;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.recommendation.AlgorithmStrategy;
import io.seldon.recommendation.ClientStrategy;
import io.seldon.recommendation.ItemFilter;
import io.seldon.recommendation.ItemIncluder;
import io.seldon.recommendation.JsOverrideClientStrategy;
import io.seldon.recommendation.RecTagClientStrategy;
import io.seldon.recommendation.SimpleClientStrategy;
import io.seldon.recommendation.VariationTestingClientStrategy;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;
import io.seldon.recommendation.filters.base.CurrentItemFilter;
import io.seldon.recommendation.filters.base.IgnoredRecsFilter;
import io.seldon.recommendation.filters.base.RecentImpressionsFilter;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Sets;

/**
 * Cache of which algorithms to use for which clients. Receives updates via ClientConfigUpdateListener
 *
 * @author firemanphil
 *         Date: 27/11/14
 *         Time: 13:55
 */
@Component
public class ClientAlgorithmStore implements ApplicationContextAware,ClientConfigUpdateListener,GlobalConfigUpdateListener {

    private static final String RECTAG = "alg_rectags";
    protected static Logger logger = Logger.getLogger(ClientAlgorithmStore.class.getName());

    private static final String ALG_KEY = "algs";
    private static final String TESTING_SWITCH_KEY = "alg_test_switch";
    private static final String TEST = "alg_test";



    private final ClientConfigHandler configHandler;
    private final GlobalConfigHandler globalConfigHandler;
    private final Set<ItemFilter> alwaysOnFilters;
    private ApplicationContext applicationContext;
    private ConcurrentMap<String, ClientStrategy> store = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Map<String, AlgorithmStrategy>> storeMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Boolean> testingOnOff = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ClientStrategy> tests = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ClientStrategy> recTagStrategies = new ConcurrentHashMap<>();
    private ClientStrategy defaultStrategy = null;
    private ConcurrentMap<String, ClientStrategy> namedStrategies = new ConcurrentHashMap<>();






    @Autowired
    public ClientAlgorithmStore (ClientConfigHandler configHandler,
                                 GlobalConfigHandler globalConfigHandler,
                                 CurrentItemFilter currentItemFilter,
                                 IgnoredRecsFilter ignoredRecsFilter,
                                 RecentImpressionsFilter recentImpressionsFilter){
        this.configHandler = configHandler;
        this.globalConfigHandler = globalConfigHandler;
        Set<ItemFilter> set = new HashSet<>();
        set.add (currentItemFilter);
        set.add(ignoredRecsFilter);
        set.add(recentImpressionsFilter);
        alwaysOnFilters = Collections.unmodifiableSet(set);
    }

    @PostConstruct
    private void init(){
        logger.info("Initializing...");
        configHandler.addListener(this);
        globalConfigHandler.addSubscriber("default_strategy", this);
//        globalConfigHandler.addSubscriber("named_strategies", this);
    }

    public ClientStrategy retrieveStrategy(String client, Collection<String> algs){
        ClientStrategy originalStrat = retrieveStrategy(client);
        return new JsOverrideClientStrategy(originalStrat, algs, applicationContext);
    }

    public ClientStrategy retrieveStrategy(String client){
        if (testRunning(client)){
            ClientStrategy strategy = tests.get(client);
            if(strategy!=null){
                return strategy;
            } else {
                logger.warn("Testing was switch on for client " + client+ " but no test was specified." +
                        " Returning default strategy");
                return defaultStrategy;
            }
        } else {
            ClientStrategy strategy;
            if(recTagStrategies.containsKey(client))
                strategy= recTagStrategies.get(client);
            else
                strategy = store.get(client);
            if(strategy!=null){
                return strategy;
            } else {
                return defaultStrategy;
            }
        }
    }

    @Override
	public void configRemoved(String client, String configKey) {
		logger.info("Received config remove for "+client+" with key "+configKey);
		if (configKey.equals(ALG_KEY)){
			store.remove(client);
			logger.info("Successfully removed "+client+" from "+ALG_KEY);
		}
		else if (configKey.equals(TESTING_SWITCH_KEY)){
			testingOnOff.remove(client);
			logger.info("Successfully removed "+client+" from "+TESTING_SWITCH_KEY);
		}
		else if (configKey.equals(TEST)) 
		{
			tests.remove(client);
			logger.info("Successfully removed "+client+" from "+TEST);
		}
		else if(configKey.equals(RECTAG)){
			recTagStrategies.remove(client);
			logger.info("Successfully removed "+client+" from "+RECTAG);
		}
		else
			logger.warn("Ignored unknow config remove for "+client+" with key "+configKey);
	}

    @Override
    public void configUpdated(String client, String configKey, String configValue) {
        SimpleModule module = new SimpleModule("StrategyDeserializerModule");
        module.addDeserializer(Strategy.class, new StrategyDeserializer());
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        if (configKey.equals(ALG_KEY)){
            logger.info("Received new algorithm config for "+ client+": "+ configValue);
            try {
                List<AlgorithmStrategy> strategies = new ArrayList<>();
                Map<String, AlgorithmStrategy> stratMap = new HashMap<>();
                AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);
                for (Algorithm algorithm : config.algorithms) {
                    AlgorithmStrategy strategy = toAlgorithmStrategy(algorithm);
                    strategies.add(strategy);
                }
                AlgorithmResultsCombiner combiner = applicationContext.getBean(
                        config.combiner,AlgorithmResultsCombiner.class);
                Map<Integer,Double> actionWeightMap = toActionWeightMap(config.actionWeights);
                store.put(client, new SimpleClientStrategy(Collections.unmodifiableList(strategies), combiner,config.diversityLevel,
                        ClientStrategy.DEFAULT_NAME,actionWeightMap));
                storeMap.put(client, Collections.unmodifiableMap(stratMap));
                logger.info("Successfully added new algorithm config for "+client);
            } catch (IOException | BeansException e) {
                logger.error("Couldn't update algorithms for client " +client, e);
            }
        } else if (configKey.equals(TESTING_SWITCH_KEY)){
            // not json as its so simple
            Boolean onOff = BooleanUtils.toBooleanObject(configValue);
            if(onOff==null){
                logger.error("Couldn't set testing switch for client "+client +", input was " +configValue);
            } else {
                logger.info("Testing switch for client " + client + " moving from '" +
                        BooleanUtils.toStringOnOff(testingOnOff.get(client)) +
                        "' to '" + BooleanUtils.toStringOnOff(onOff)+"'");
                testingOnOff.put(client, BooleanUtils.toBooleanObject(configValue));
            }
        } else if (configKey.equals(TEST)) {
                logger.info("Received new testing config for " + client + ":" + configValue);
                try {
                    TestConfig config = mapper.readValue(configValue, TestConfig.class);
                    List<VariationTestingClientStrategy.Variation> variations = new ArrayList<>();
                    for (TestVariation var : config.variations){
                        List<AlgorithmStrategy> strategies = new ArrayList<>();
                        for (Algorithm alg : var.config.algorithms){
                            AlgorithmStrategy strategy = toAlgorithmStrategy(alg);
                            strategies.add(strategy);
                        }
                        AlgorithmResultsCombiner combiner = applicationContext.getBean(
                                var.config.combiner,AlgorithmResultsCombiner.class);
                        Map<Integer,Double> actionWeightMap = toActionWeightMap(var.config.actionWeights);
                        variations.add(new VariationTestingClientStrategy.Variation(
                                new SimpleClientStrategy(Collections.unmodifiableList(strategies),
                                        combiner, var.config.diversityLevel, var.label,actionWeightMap),
                                new BigDecimal(var.ratio)));

                    }
                    tests.put(client,VariationTestingClientStrategy.build(variations));
                    logger.info("Succesfully added " + variations.size() + " variation test for "+ client);
                } catch (NumberFormatException | IOException e) {
                    logger.error("Couldn't add test for client " +client, e);
                }
        } else if(configKey.equals(RECTAG)){
            logger.info("Received new rectag config for "+ client + ": " +configValue);
            try{
                RecTagConfig config = mapper.readValue(configValue, RecTagConfig.class);
                if(config.defaultStrategy==null){
                    logger.error("Couldn't read rectag config as there was no default alg");
                    return;
                }

                ClientStrategy defStrategy = toStrategy(config.defaultStrategy);
                Map<String, ClientStrategy> recTagStrats = new HashMap<>();
                for (Map.Entry<String, Strategy> entry : config.recTagToStrategy.entrySet() ){
                    recTagStrats.put(entry.getKey(),toStrategy(entry.getValue()));
                }
                recTagStrategies.put(client, new RecTagClientStrategy(defStrategy, recTagStrats));
                logger.info("Successfully added rec tag strategy for " + client);
            } catch (NumberFormatException | IOException e) {
                logger.error("Couldn't add rectag strategy for client " +client, e);
            }

        }
    }

    private ClientStrategy toStrategy(Strategy jsonStrategy) {
        if (jsonStrategy instanceof TestConfig){
            TestConfig jsonStrategyTest = (TestConfig) jsonStrategy;
            List<VariationTestingClientStrategy.Variation> variations = new ArrayList<>();
            for (TestVariation var : jsonStrategyTest.variations){
                List<AlgorithmStrategy> strategies = new ArrayList<>();
                for (Algorithm alg : var.config.algorithms){
                    AlgorithmStrategy strategy = toAlgorithmStrategy(alg);
                    strategies.add(strategy);
                }
                AlgorithmResultsCombiner combiner = applicationContext.getBean(
                        var.config.combiner,AlgorithmResultsCombiner.class);
                Map<Integer,Double> actionWeightMap = toActionWeightMap(var.config.actionWeights);
                variations.add(new VariationTestingClientStrategy.Variation(
                        new SimpleClientStrategy(Collections.unmodifiableList(strategies),
                                combiner, var.config.diversityLevel, var.label,actionWeightMap),
                        new BigDecimal(var.ratio)));

            }
            return VariationTestingClientStrategy.build(variations);

        } else {
            AlgorithmConfig jsonStrategyAlg = (AlgorithmConfig) jsonStrategy;
            List<AlgorithmStrategy> defaultAlgStrategies = new ArrayList<>();
            for (Algorithm alg : jsonStrategyAlg.algorithms){
                AlgorithmStrategy strategy = toAlgorithmStrategy(alg);
                defaultAlgStrategies.add(strategy);
            }
            AlgorithmResultsCombiner defCombiner = applicationContext.getBean(
                    jsonStrategyAlg.combiner, AlgorithmResultsCombiner.class);
            Map<Integer,Double> defActionWeightMap = toActionWeightMap(jsonStrategyAlg.actionWeights);
            return new SimpleClientStrategy(defaultAlgStrategies, defCombiner,
                    jsonStrategyAlg.diversityLevel,"-",defActionWeightMap);
        }
    }

    private AlgorithmStrategy toAlgorithmStrategy(Algorithm algorithm) {
        Set<ItemIncluder> includers = retrieveIncluders(algorithm.includers);
        Set<ItemFilter> filters = retrieveFilters(algorithm.filters);
        ItemRecommendationAlgorithm alg = applicationContext.getBean(algorithm.name, ItemRecommendationAlgorithm.class);
        Map<String, String> config  = toConfigMap(algorithm.config);
        return new AlgorithmStrategy(alg, includers, filters,
                algorithm.config ==null ? new HashMap<String, String>(): config , algorithm.name);
    }

    private Map<String, String> toConfigMap(List<ConfigItem> config) {
        Map<String, String> configMap = new HashMap<>();
        if (config==null) return configMap;
        for (ConfigItem item : config){
            configMap.put(item.name,item.value);
        }
        return configMap;
    }
    
    private Map<Integer,Double> toActionWeightMap(List<ActionWeightItem> weights) {
    	Map<Integer,Double> weightMap = new HashMap<Integer,Double>();
    	if (weights == null) return Collections.unmodifiableMap(weightMap);
    	for(ActionWeightItem item : weights){
    		weightMap.put(Integer.parseInt(item.type), item.value);
    	}
    	return Collections.unmodifiableMap(weightMap);
    }

    private Set<ItemIncluder> retrieveIncluders(List<String> includers) {

        Set<ItemIncluder> includerSet = new HashSet<>();
        if(includers==null) return includerSet;
        for (String includer : includers){
            includerSet.add(applicationContext.getBean(includer, ItemIncluder.class));
        }
        return includerSet;
    }

    private Set<ItemFilter> retrieveFilters(List<String> filters) {
        Set<ItemFilter> filterSet = new HashSet<>();
        if(filters==null) return alwaysOnFilters;
        for (String filter : filters){
            filterSet.add(applicationContext.getBean(filter, ItemFilter.class));
        }
        return Sets.union(filterSet, alwaysOnFilters);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        StringBuilder builder= new StringBuilder("Available algorithms: \n");
        for (ItemRecommendationAlgorithm inc : applicationContext.getBeansOfType(ItemRecommendationAlgorithm.class).values()){
            builder.append('\t');
            builder.append(inc.getClass());
            builder.append('\n');
        }
        logger.info(builder.toString());
        builder= new StringBuilder("Available includers: \n");
        for (ItemIncluder inc : applicationContext.getBeansOfType(ItemIncluder.class).values()){
            builder.append('\t');
            builder.append(inc.getClass());
            builder.append('\n');
        }
        logger.info(builder.toString());
        builder = new StringBuilder("Available filters: \n" );
        for (ItemFilter filt: applicationContext.getBeansOfType(ItemFilter.class).values()){
            builder.append('\t');
            builder.append(filt.getClass());
            builder.append('\n');

        }
        logger.info(builder.toString());
        for (AlgorithmResultsCombiner filt: applicationContext.getBeansOfType(AlgorithmResultsCombiner.class).values()){
            builder.append('\t');
            builder.append(filt.getClass());
            builder.append('\n');

        }
        builder = new StringBuilder("Available combiners: \n" );
        logger.info(builder.toString());
    }

    private boolean testRunning(String client){
        return testingOnOff.get(client)!=null && testingOnOff.get(client) && tests.get(client)!=null;
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        configValue = StringUtils.strip(configValue);
        logger.info("KEY WAS " + configKey);
        logger.info("Received new default strategy: " + configValue);
        
        if (StringUtils.length(configValue) == 0) {
        	logger.warn("*WARNING* no default strategy is set!");
        } else {
            try {
                ObjectMapper mapper = new ObjectMapper();
                List<AlgorithmStrategy> strategies = new ArrayList<>();
                AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);

                for (Algorithm alg : config.algorithms){
                    strategies.add(toAlgorithmStrategy(alg));
                }
                AlgorithmResultsCombiner combiner = applicationContext.getBean(
                        config.combiner,AlgorithmResultsCombiner.class);
                Map<Integer,Double> actionWeightMap = toActionWeightMap(config.actionWeights);
                ClientStrategy strat = new SimpleClientStrategy(strategies, combiner, config.diversityLevel,"-",actionWeightMap);
                defaultStrategy = strat;
                logger.info("Successfully changed default strategy.");
            } catch (IOException e){
                logger.error("Problem changing default strategy ", e);
            }
        }
    }

    // classes for json translation
    public static class TestConfig extends Strategy {
        public List<TestVariation> variations;
    }


    public static class RecTagConfig {
        public Map<String, Strategy> recTagToStrategy;
        public Strategy defaultStrategy;
    }
    private static class TestVariation{
        public String label;
        public String ratio;
        public AlgorithmConfig config;
    }
    public abstract static class Strategy {}




    public static class AlgorithmConfig extends Strategy {
        public List<Algorithm> algorithms;
        public String combiner;
        public Double diversityLevel;
        public List<ActionWeightItem> actionWeights;
    }

    public static class ConfigItem {
        public String name;
        public String value;
    }
    
    public static class ActionWeightItem {
    	public String type;
    	public Double value;
    }

    public static class Algorithm {

        public String name;
        public List<String> includers;
        public List<String> filters;
        public List<ConfigItem> config;
    }

	

}