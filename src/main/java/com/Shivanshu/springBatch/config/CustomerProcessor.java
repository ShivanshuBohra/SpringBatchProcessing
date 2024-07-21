package com.Shivanshu.springBatch.config;

import org.springframework.batch.item.ItemProcessor;

import com.Shivanshu.springBatch.entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer, Customer>{

	@Override
	public Customer process(Customer customer) throws Exception {
	
		//PROCESS THE DATA > ONLY DATA FROM CSV WITH UNITED STATES COUNTRY WILL BE PASSED
		if(customer.getCountry().equals("United States")) {
            return customer;
        }else{
            return null;
        }
      
	}
	
	

}
