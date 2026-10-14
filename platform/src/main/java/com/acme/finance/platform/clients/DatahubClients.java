package com.acme.finance.platform.clients;

import com.acme.datahub.v1.WorkspaceQueryServiceGrpc;
import com.acme.datahub.v1.WorkspaceServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the datahub contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DatahubClients {
    @Bean
    public WorkspaceQueryServiceGrpc.WorkspaceQueryServiceBlockingStub datahubWorkspaceQueryServiceBlocking(ChannelRegistry channels) {
        return WorkspaceQueryServiceGrpc.newBlockingStub(channels.lookup("acme.datahub.v1.WorkspaceQueryService"));
    }

    @Bean
    public WorkspaceServiceGrpc.WorkspaceServiceStub datahubWorkspaceServiceAsync(ChannelRegistry channels) {
        return WorkspaceServiceGrpc.newStub(channels.lookup("acme.datahub.v1.WorkspaceService"));
    }

    @Bean
    public WorkspaceServiceGrpc.WorkspaceServiceBlockingStub datahubWorkspaceServiceBlocking(ChannelRegistry channels) {
        return WorkspaceServiceGrpc.newBlockingStub(channels.lookup("acme.datahub.v1.WorkspaceService"));
    }
}
