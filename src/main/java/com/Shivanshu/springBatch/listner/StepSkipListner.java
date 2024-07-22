package com.Shivanshu.springBatch.listner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import com.Shivanshu.springBatch.entity.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


//Implement SkipListener to identify Error and which event has error
// In this class adding a Logger in of skip 
public class StepSkipListner  implements SkipListener<Customer, Number>{

	//StepExecution class has data like read count write count etc.
	
	Logger logger = LoggerFactory.getLogger(StepSkipListner.class);
	
	 // for failure in Reader
	@Override 
	public void onSkipInRead(Throwable t) {
		logger.info("A failure on read {}" ,t.getMessage());
	}

	 // for failure in Writer
	@Override
	public void onSkipInWrite(Number item, Throwable t) {	
        logger.info("A failure on write {} , {}", t.getMessage(), item);		
	}

	 // for failure in Processor
	//   @SneakyThrows can be use to handle throws error in LOMBOK
	@Override
	public void onSkipInProcess(Customer item, Throwable t) {
		  try {
			logger.info("Item {}  was skipped due to the exception  {}", 
					  new ObjectMapper().writeValueAsString(item),
			            t.getMessage());
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

}
