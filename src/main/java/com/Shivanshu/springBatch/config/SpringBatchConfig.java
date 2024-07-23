package com.Shivanshu.springBatch.config;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.Shivanshu.springBatch.entity.Customer;
import com.Shivanshu.springBatch.repository.CustomerRepository;

// refer SpringBatch Flow in Resources foe spring batch architecture
//1>Job Launcher launches the job
//2>A job can have multiple steps 
//3> Each step has its reader, processor and writer .
//4> reader reads the file from source , processor process the data what needs to be filtered
//and writes writes the file to destination
@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	@Autowired
    private JobBuilderFactory jobBuilderFactory;

	@Autowired
    private StepBuilderFactory stepBuilderFactory;

	@Autowired
    private CustomerRepository customerRepository;


	// Read the file from Source
    @Bean
    // By default scope is singleton but we want new Bean for each step.
    @StepScope  
    public FlatFileItemReader<Customer> reader(@Value("#{jobParameters[fullPathFileName]}") String pathToFIle) {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
       // itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setResource(new FileSystemResource(new File(pathToFIle)));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    // Separate data from CSV file
    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }

    // Process the data
    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    
    //Writes the data in destination
    @Bean
    public RepositoryItemWriter<Customer> writer() {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1(FlatFileItemReader<Customer> reader) {
        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
                .reader(reader)
                .processor(processor())
                .writer(writer())
                // added default skip policy  for fault tolerance
//                .faultTolerant()
//                .skipLimit(100)    // how many times the exxeption to be ignored
//                .skip(NumberFormatException.class) // which exception to skip
//                .noSkip(IllegalArgumentException.class) // rollback and not skip ehich exception
                
                // ADD CUSTOM SKIP POLICY FOR FAULT TOLERANCE
                .faultTolerant()
                .skipPolicy(skipPolicy())
                
                //added below for asynchronous call all threads will run in parallel
                .taskExecutor(taskExecutor())
                .build();
    }

    // Run the JOb , It can have multiple steps like step1, steo2 etc
    @Bean
    public Job runJob(FlatFileItemReader<Customer> reader) {
        return jobBuilderFactory.get("importCustomers")
                .flow(step1(reader)).end().build();

    }

    // To asynchronously execute the task , define n of threads that will work on task
    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }
   
    @Bean
    public SkipPolicy skipPolicy() {
    	return new ExceptionSkipPolicy();
    }

}