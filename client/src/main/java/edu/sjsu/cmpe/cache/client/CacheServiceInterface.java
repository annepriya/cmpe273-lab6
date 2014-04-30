package edu.sjsu.cmpe.cache.client;

/**
 * Cache Service Interface
 * 
 */
public interface CacheServiceInterface {
    public String get(Long key);

    public void put(Long key, String value);
}
