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

package io.seldon.servlet;

import io.seldon.api.state.ZkCuratorHandler;
import io.seldon.api.state.zk.ZkClientConfigHandler;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.memcache.SecurityHashPeer;

import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class ResourceManagerListener  implements ServletContextListener {

	private static Logger logger = Logger.getLogger( ResourceManagerListener.class.getName() );
	

    private ZkClientConfigHandler zkClientConfigHandler;

    public void contextInitialized(ServletContextEvent sce)
    {
        logger.info("**********************  STARTING API-SERVER INITIALISATION **********************");
		JDOFactory jdoFactory = null;
    	try
    	{  
    		final WebApplicationContext springContext = WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
			jdoFactory = (JDOFactory) springContext.getBean("JDOFactory");

       
    		zkClientConfigHandler =(ZkClientConfigHandler) springContext.getBean("zkClientConfigHandler");
    		SecurityHashPeer.initialise();
    	
    		ZkCuratorHandler curatorHandler = ZkCuratorHandler.getPeer();
    		
			zkClientConfigHandler.contextIntialised();
    		logger.info("**********************  ENDING API-SERVER INITIALISATION **********************");
    	}
    	catch( IOException ex )
    	{
             logger.error("IO Exception",ex);
    	}
    	catch (Exception ex)
    	{
    		logger.error("Exception at resource initialization",ex);
    	}
    	catch (Throwable e)
    	{
    		logger.error("Throwable during initialization ",e);
    	}
    	finally
        {
			if(jdoFactory!=null)
				jdoFactory.cleanupPM();
        }
    }
    
    public void contextDestroyed(ServletContextEvent sce)
    {
        
        ZkCuratorHandler.shutdown();
    }

}
