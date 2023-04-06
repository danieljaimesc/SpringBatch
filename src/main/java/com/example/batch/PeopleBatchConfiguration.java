package com.example.batch;

import com.example.model.People;
import com.example.model.PeopleDTO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class PeopleBatchConfiguration {
    @Autowired
    public PeopleItemProcessor peopleItemProcessor;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    PlatformTransactionManager transactionManager;

    public FlatFileItemReader<PeopleDTO> peopleCSVItemReader(String fname) {
        return new FlatFileItemReaderBuilder<PeopleDTO>().name("personaCSVItemReader")
                .resource(new ClassPathResource(fname))
                .linesToSkip(1)
                .delimited()
                .names("id", "nombre", "apellidos", "correo", "sexo", "ip")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<PeopleDTO>() {
                    {
                        setTargetType(PeopleDTO.class);
                    }
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<People> personaDBItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<People>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO personas VALUES (:id,:nombre,:correo,:ip)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step importCSV2DBStep1(JdbcBatchItemWriter<People> personaDBItemWriter) {
        return new StepBuilder("importCSV2DBStep1", jobRepository)
                .<PeopleDTO, People>chunk(10, transactionManager)
                .reader(peopleCSVItemReader("personas-1.csv"))
                .processor(peopleItemProcessor)
                .writer(personaDBItemWriter)
                .build();
    }

    @Bean
    public Job personasJob(PeopleJobListener listener, Step importCSV2DBStep1) {
        return new JobBuilder("personasJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(importCSV2DBStep1)
                .build();
    }


}