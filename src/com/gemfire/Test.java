package com.gemfire;

import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

public class Test {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	Cache cache = new CacheFactory().set("locators", "localhost[40000]")
		.set("mcast-port", "0").set("log-level", "error").create();
	Region<String,String> exampleRegion = cache.getRegion("regionA");
	System.out.println("aaaaaaaaa-----"+exampleRegion.keySet());
	cache.close();
    }

}
