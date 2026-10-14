package com.acme.finance.platform;

import io.grpc.Context;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/** Virtual threads serve blocking RPC work; a bounded executor handles background application events. */
@Configuration(proxyBeanMethods = false)
@EnableAsync
public class ExecutionConfiguration {
    @Bean(destroyMethod = "close")
    public ExecutorService rpcThreads() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public Executor rpcExecutor(ExecutorService rpcThreads) {
        return Context.currentContextExecutor(rpcThreads);
    }

    @Bean(name = "eventExecutor")
    public ThreadPoolTaskExecutor eventExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(256);
        executor.setThreadNamePrefix("finance-event-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        return executor;
    }
}
