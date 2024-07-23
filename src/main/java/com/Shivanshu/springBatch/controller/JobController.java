package com.Shivanshu.springBatch.controller;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private Job job;
    
    // while passing dynamic file needs to be stored in local path or DB for processing
  public static final String TEMP_STORAGE =  "/Users/shivanshu/Desktop/batch-files";

    @PostMapping("/importCustomers")
    public void importCsvToDBJob(@RequestParam("file") MultipartFile multipartFile) {
    	
    	 // file  -> path we don't know
        //copy the file to some storage in your VM : get the file path
        //copy the file to DB : get the file path
    	try {
    	
        String originalFileName = multipartFile.getOriginalFilename();
        File fileToImport = new File(TEMP_STORAGE + originalFileName);
        multipartFile.transferTo(fileToImport);
    	
    	JobParameters jobParameters = new JobParametersBuilder()
                .addString("fullPathFileName", TEMP_STORAGE + originalFileName)
                .addDate("startAt", new Date()).toJobParameters();
        
            jobLauncher.run(job, jobParameters);
       
            // USe below if want to delete file from temp storage after job is executed
//          if(execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED)){
//          //delete the file from the TEMP_STORAGE
//          Files.deleteIfExists(Paths.get(TEMP_STORAGE + originalFileName));
//      }
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException | JobParametersInvalidException  | IOException e) {
            e.printStackTrace();
        }
    }
}