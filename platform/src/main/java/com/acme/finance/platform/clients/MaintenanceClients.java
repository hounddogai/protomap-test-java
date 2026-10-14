package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.maintenance.v1.WorkOrderQueryServiceGrpc;
import com.acme.maintenance.v1.WorkOrderServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the maintenance contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class MaintenanceClients {
    @Bean
    public WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceStub maintenanceWorkOrderCalculationServiceAsync(ChannelRegistry channels) {
        return WorkOrderCalculationServiceGrpc.newStub(channels.lookup("acme.maintenance.v1.WorkOrderCalculationService"));
    }

    @Bean
    public WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking(ChannelRegistry channels) {
        return WorkOrderCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.maintenance.v1.WorkOrderCalculationService"));
    }

    @Bean
    public WorkOrderQueryServiceGrpc.WorkOrderQueryServiceBlockingStub maintenanceWorkOrderQueryServiceBlocking(ChannelRegistry channels) {
        return WorkOrderQueryServiceGrpc.newBlockingStub(channels.lookup("acme.maintenance.v1.WorkOrderQueryService"));
    }

    @Bean
    public WorkOrderServiceGrpc.WorkOrderServiceBlockingStub maintenanceWorkOrderServiceBlocking(ChannelRegistry channels) {
        return WorkOrderServiceGrpc.newBlockingStub(channels.lookup("acme.maintenance.v1.WorkOrderService"));
    }
}
