package com.acme.finance.platform.clients;

import com.acme.employees.v1.EmployeeQueryServiceGrpc;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the employees contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class EmployeesClients {
    @Bean
    public EmployeeQueryServiceGrpc.EmployeeQueryServiceBlockingStub employeesEmployeeQueryServiceBlocking(ChannelRegistry channels) {
        return EmployeeQueryServiceGrpc.newBlockingStub(channels.lookup("acme.employees.v1.EmployeeQueryService"));
    }

    @Bean
    public EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking(ChannelRegistry channels) {
        return EmployeeServiceGrpc.newBlockingStub(channels.lookup("acme.employees.v1.EmployeeService"));
    }
}
