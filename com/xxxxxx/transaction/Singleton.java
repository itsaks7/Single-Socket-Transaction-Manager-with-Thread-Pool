package com.xxxxxx.transaction;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class Singleton {

	private static Singleton singleton = new Singleton();
	StandardPBEStringEncryptor encryptor;
	private Singleton(){}

	
	public static Singleton getSingleton(){
		return singleton;
	}
	
	public StandardPBEStringEncryptor getEncryptor(){
		return encryptor;
	}
	
	
	public void setEncryptor(StandardPBEStringEncryptor encryptor) {
		this.encryptor = encryptor;
	}	
}
