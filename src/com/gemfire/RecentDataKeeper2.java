package com.gemfire;

import static com.gemstone.gemfire.cache.RegionShortcut.REPLICATE_PERSISTENT;

import java.util.Set;

//import com.cnpc.a11.a11rtdataservice.connection.DBConnection;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

//import org.datacontract.schemas._2004._07.Honeywell_PHD_Interface_Contracts.TagData;

public class RecentDataKeeper2 {

    static Region<String, String> tagdatas;
    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	RecentDataKeeper2 keeper=new RecentDataKeeper2();
	Cache cache = new CacheFactory().set("locators", "localhost[40000]")
		.set("mcast-port", "0").set("log-level", "error").create();
	LoggingCacheListener listener = new LoggingCacheListener();
	tagdatas = cache.<String, String>createRegionFactory(REPLICATE_PERSISTENT)
		      .addCacheListener(listener)
		      .create("tagdatas2");
	int i=0,j=0;
	while (i<100) {
	    i++;
	    j++;
	    keeper.addTagdata(i+"",j+"");
	    
	}
	System.out.println(keeper.getTagDatas());
	for (int j2 = 0; j2 < tagdatas.size(); j2++) {
	    System.out.println(keeper.getTagdata(j2+""));
	}
    }
    
    public void addTagdata(String key, String value) {
	tagdatas.put(key, value);
	System.out.println("插入----key:"+key+";value:"+value);
    }
    
    public Set<String> getTagDatas() {
	    return tagdatas.keySet();
    }
    
    public String getTagdata(String key) {
	    return "查询----key:"+key+";value:"+tagdatas.get(key);
    }
}
