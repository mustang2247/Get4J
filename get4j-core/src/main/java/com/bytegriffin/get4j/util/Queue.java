package com.bytegriffin.get4j.util;

import java.util.LinkedList;

public interface Queue<E> {

	void add(E e) ;
	
	void add(String queueName, E e) ;
	
	E get(int index);
	
	long size();
	 
	 long size(String queueName);
	 
	 void clear();
	 
	 void clear(String queueName);
	 
	 boolean isEmpty();
	 
	 boolean isEmpty(String queueName);
	 
	 E outFirst();
	 
	 E outFirst(String queueName);
	 
	 boolean contains(E e);
	 
	 boolean contains(String queueName, E e);

	 Queue<E> getQueue(String queueName);
	 
	 LinkedList<E> getList();

}
