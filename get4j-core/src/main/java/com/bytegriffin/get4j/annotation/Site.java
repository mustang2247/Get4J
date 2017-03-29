package com.bytegriffin.get4j.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Site {
	
	int thread() default 1;
	
	String url() ;//没有default值必须要设置

	String startTime() default "";

	long interval() default 0;

	long sleep() default 0;

	String proxy() default "";
	
	String userAgent() default "";

	String resourceSelector() default "";

	String downloadDisk() default "";

	String downloadHdfs() default "";
	
	boolean javascriptSupport() default false;

	String parser() default "";
	
	String jdbc() default "";
	
	String lucene() default "";
	
	String hbase() default "";

}