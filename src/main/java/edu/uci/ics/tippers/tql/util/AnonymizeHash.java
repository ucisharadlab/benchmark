package edu.uci.ics.tippers.tql.util;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class AnonymizeHash {
		
	public static final int MAX_BUILDING_OCCUPANTS = 50000;
	
	public static final double COLLISION_RATE = 0.01;
	
	/**
	 * Using Google Guava https://github.com/google/guava/wiki/HashingExplained
	 * @param toBeAnonymized
	 * @return
	 */
	public static String anonymize(String toBeAnonymized) {
		return Long.toString((long) (Hashing.md5().hashString(toBeAnonymized, Charsets.UTF_8).asLong() 
				% (MAX_BUILDING_OCCUPANTS * (1 - COLLISION_RATE))));
	}
}
