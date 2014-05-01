package edu.sjsu.cmpe.cache.client;

/**
 * Cache Service Interface
 * 
 */
public interface CacheServiceInterface {
    public String get(Integer key);

    public void put(Integer key, String value);
    
    public String getServer();
}
