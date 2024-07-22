package com.Shivanshu.springBatch.config;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

//Extent Skippolicy to to write own skip policy in case of exception
public class ExceptionSkipPolicy implements SkipPolicy{

	@Override
	public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
		// return true for NumberFormatException 
		// to add skip limit here add && skipCount =2
		return t instanceof NumberFormatException;
	}

}
