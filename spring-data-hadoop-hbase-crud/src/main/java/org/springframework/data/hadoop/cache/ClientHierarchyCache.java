package org.springframework.data.hadoop.cache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientHierarchyCache implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6935861163769453496L;
	private Map<String, List<String>> clientIdToFxmmCode = new HashMap<String, List<String>>();
	private Map<String, List<String>> clientIdToCashAccountNumber = new HashMap<String, List<String>>();
	private Map<String, List<String>> clientIdToSecurityAccountNumber = new HashMap<String, List<String>>();
	
	private static ClientHierarchyCache cache = new ClientHierarchyCache();
	
	private ClientHierarchyCache(){
		
	}
	
	public void initCache(ClientHierarchyCache cache){
		this.cache = cache;
	}
	
	public static ClientHierarchyCache getInstance(){
		return cache;
	}
	
	public Map<String, List<String>> getClientIdToFxmmCode(){
		return this.clientIdToFxmmCode;
	}
	
	public Map<String, List<String>> getClientIdToCashAccountNumber(){
		return this.clientIdToCashAccountNumber;
	}
	
	public Map<String, List<String>> getClientIdToSecurityAccountNumber(){
		return this.clientIdToSecurityAccountNumber;
	}
	
	public ClientHierarchyCache getAll(){
		return cache;
	}
}
