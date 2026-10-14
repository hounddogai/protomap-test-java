package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.learning.v1.CourseQueryServiceGrpc;
import com.acme.learning.v1.CourseServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the learning contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LearningClients {
    @Bean
    public CourseQueryServiceGrpc.CourseQueryServiceBlockingStub learningCourseQueryServiceBlocking(ChannelRegistry channels) {
        return CourseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.learning.v1.CourseQueryService"));
    }

    @Bean
    public CourseServiceGrpc.CourseServiceBlockingStub learningCourseServiceBlocking(ChannelRegistry channels) {
        return CourseServiceGrpc.newBlockingStub(channels.lookup("acme.learning.v1.CourseService"));
    }
}
