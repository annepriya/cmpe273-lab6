package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Client {
	private final SortedMap<Long, String> cacheLookUp = new TreeMap<Long, String>();
	private final HashFunction hashFunction;
	
	
	public Client(HashFunction hf,List<String> servers){
	
		this.hashFunction=hf;
		for(String server : servers){
			cacheLookUp.put(hashFunction.hashString(server + 1).asLong(),
                     server);
			
		}
		
		
		
	}
	
	public String getServer(Object key){
		 if (cacheLookUp.isEmpty()) {
	            return null;
	        }
	        long hash = hashFunction.hashString(key.toString()).asLong();
	        if (!cacheLookUp.containsKey(hash)) {
	            SortedMap<Long, String> tailMap = cacheLookUp.tailMap(hash);
	            hash = tailMap.isEmpty() ? cacheLookUp.firstKey() : tailMap.firstKey();
	        }
	        return cacheLookUp.get(hash);
	    }
	

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        int bucket=0;
        
        List<String> servers = new ArrayList<String>();
        servers.add("http://localhost:3000");
        servers.add("http://localhost:3001");
        servers.add("http://localhost:3002");

       HashMap<Long,String> keyValue=new HashMap<Long, String>();
        keyValue.put(new Long(1), "a");
        keyValue.put(new Long(2), "b");
        keyValue.put(new Long(3), "c");
        keyValue.put(new Long(4), "d");
        keyValue.put(new Long(5), "e");
        keyValue.put(new Long(6), "f");
        keyValue.put(new Long(7), "g");
        keyValue.put(new Long(8), "h");
        keyValue.put(new Long(9), "i");
        keyValue.put(new Long(10), "j");
        
        HashFunction hf = Hashing.md5();
        
        Client client=new Client(hf, servers);
        Long key=0L; 
        String value=null;
        String server=null;
        for (Map.Entry<Long,String> entry : keyValue.entrySet()) {
        	   key = entry.getKey();
        	   value = entry.getValue();
        	  // do stuff
        	  
        	  bucket=Hashing.consistentHash(hf.hashString(key.toString()), servers.size());
              server=client.getServer(bucket);
              CacheServiceInterface cache=new DistributedCacheService(
                      server);
              cache.put(key, value);
              
              System.out.println(" put "+key+"to -->"+server);

              String valueToPrint = cache.get(key);
              System.out.println("get -" + valueToPrint+"- from server"+server);
        	}
        
      
       
        System.out.println("Existing Cache Client...");
    }

}
