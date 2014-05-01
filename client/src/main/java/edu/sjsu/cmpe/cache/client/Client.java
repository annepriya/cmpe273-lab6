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
	private final SortedMap<Integer, String> cacheLookUp = new TreeMap<Integer, String>();
	private final HashFunction hashFunction;
	
	
	public Client(HashFunction hf,List<String> servers){
	
		this.hashFunction=hf;
		for(String server : servers){
			cacheLookUp.put(hashFunction.hashString(server).asInt(),
                     server);
			
		}
		
		
		
	}
	
	public String getServer(Object key){
		 if (cacheLookUp.isEmpty()) {
	            return null;
	        }
	        int hash = hashFunction.hashString(key.toString()).asInt();
	        if (!cacheLookUp.containsKey(hash)) {
	            SortedMap<Integer, String> tailMap = cacheLookUp.tailMap(hash);
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

       HashMap<Integer,String> keyValue=new HashMap<Integer, String>();
        keyValue.put(1, "a");
        keyValue.put(2, "b");
        keyValue.put(3, "c");
        keyValue.put(4, "d");
        keyValue.put(5, "e");
        keyValue.put(6, "f");
        keyValue.put(7, "g");
        keyValue.put(8, "h");
        keyValue.put(9, "i");
        keyValue.put(10, "j");
        
        HashFunction hf = Hashing.md5();
        
        Client client=new Client(hf, servers);
        Integer key=0; 
        String value=null;
        String server=null;
       
        for (Map.Entry<Integer,String> entry : keyValue.entrySet()) {
        	   key = entry.getKey();
        	   value = entry.getValue();
        	 
        	  bucket=Hashing.consistentHash(hf.hashString(key.toString()), servers.size());
             server=client.getServer(bucket);
              CacheServiceInterface cache=new DistributedCacheService(
                      server);
            
              cache.put(key, value);
              
              System.out.println(" put "+key+" and "+value+" to -->"+cache.getServer());

              String valueToPrint = cache.get(key);
              System.out.println("get("+key+")->" + valueToPrint+" from server"+cache.getServer());
        	}
        
       
       
      
       
        System.out.println("Existing Cache Client...");
    }

}
