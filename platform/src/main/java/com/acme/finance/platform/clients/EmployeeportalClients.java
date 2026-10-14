package com.acme.finance.platform.clients;

import com.acme.employeeportal.v1.PortalHomeQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the employeeportal contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class EmployeeportalClients {
    @Bean
    public PortalHomeQueryServiceGrpc.PortalHomeQueryServiceBlockingStub employeeportalPortalHomeQueryServiceBlocking(ChannelRegistry channels) {
        return PortalHomeQueryServiceGrpc.newBlockingStub(channels.lookup("acme.employeeportal.v1.PortalHomeQueryService"));
    }

    @Bean
    public PortalHomeQueryServiceGrpc.PortalHomeQueryServiceFutureStub employeeportalPortalHomeQueryServiceFuture(ChannelRegistry channels) {
        return PortalHomeQueryServiceGrpc.newFutureStub(channels.lookup("acme.employeeportal.v1.PortalHomeQueryService"));
    }
}
