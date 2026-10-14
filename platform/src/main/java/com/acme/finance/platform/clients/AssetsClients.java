package com.acme.finance.platform.clients;

import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLifecycleServiceGrpc;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationLifecycleServiceGrpc;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the assets contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AssetsClients {
    @Bean
    public AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceStub assetsAssetTransferApprovalServiceAsync(ChannelRegistry channels) {
        return AssetTransferApprovalServiceGrpc.newStub(channels.lookup("acme.assets.v1.AssetTransferApprovalService"));
    }

    @Bean
    public AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceBlockingStub assetsAssetTransferApprovalServiceBlocking(ChannelRegistry channels) {
        return AssetTransferApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferApprovalService"));
    }

    @Bean
    public AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking(ChannelRegistry channels) {
        return AssetTransferCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferCalculationService"));
    }

    @Bean
    public AssetTransferLifecycleServiceGrpc.AssetTransferLifecycleServiceBlockingStub assetsAssetTransferLifecycleServiceBlocking(ChannelRegistry channels) {
        return AssetTransferLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferLifecycleService"));
    }

    @Bean
    public AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking(ChannelRegistry channels) {
        return AssetTransferReportingServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferReportingService"));
    }

    @Bean
    public AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking(ChannelRegistry channels) {
        return AssetTransferSyncServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferSyncService"));
    }

    @Bean
    public AssetTransferSyncServiceGrpc.AssetTransferSyncServiceFutureStub assetsAssetTransferSyncServiceFuture(ChannelRegistry channels) {
        return AssetTransferSyncServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.AssetTransferSyncService"));
    }

    @Bean
    public AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking(ChannelRegistry channels) {
        return AssetTransferValidationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferValidationService"));
    }

    @Bean
    public AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceBlockingStub assetsAssetTransferWorkflowServiceBlocking(ChannelRegistry channels) {
        return AssetTransferWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.AssetTransferWorkflowService"));
    }

    @Bean
    public DepreciationApprovalServiceGrpc.DepreciationApprovalServiceBlockingStub assetsDepreciationApprovalServiceBlocking(ChannelRegistry channels) {
        return DepreciationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationApprovalService"));
    }

    @Bean
    public DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking(ChannelRegistry channels) {
        return DepreciationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationCalculationService"));
    }

    @Bean
    public DepreciationLifecycleServiceGrpc.DepreciationLifecycleServiceBlockingStub assetsDepreciationLifecycleServiceBlocking(ChannelRegistry channels) {
        return DepreciationLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationLifecycleService"));
    }

    @Bean
    public DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking(ChannelRegistry channels) {
        return DepreciationQueryServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationQueryService"));
    }

    @Bean
    public DepreciationQueryServiceGrpc.DepreciationQueryServiceFutureStub assetsDepreciationQueryServiceFuture(ChannelRegistry channels) {
        return DepreciationQueryServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.DepreciationQueryService"));
    }

    @Bean
    public DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking(ChannelRegistry channels) {
        return DepreciationReportingServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationReportingService"));
    }

    @Bean
    public DepreciationValidationServiceGrpc.DepreciationValidationServiceStub assetsDepreciationValidationServiceAsync(ChannelRegistry channels) {
        return DepreciationValidationServiceGrpc.newStub(channels.lookup("acme.assets.v1.DepreciationValidationService"));
    }

    @Bean
    public DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking(ChannelRegistry channels) {
        return DepreciationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationValidationService"));
    }

    @Bean
    public DepreciationValidationServiceGrpc.DepreciationValidationServiceFutureStub assetsDepreciationValidationServiceFuture(ChannelRegistry channels) {
        return DepreciationValidationServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.DepreciationValidationService"));
    }

    @Bean
    public DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceStub assetsDepreciationWorkflowServiceAsync(ChannelRegistry channels) {
        return DepreciationWorkflowServiceGrpc.newStub(channels.lookup("acme.assets.v1.DepreciationWorkflowService"));
    }

    @Bean
    public DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking(ChannelRegistry channels) {
        return DepreciationWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DepreciationWorkflowService"));
    }

    @Bean
    public DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking(ChannelRegistry channels) {
        return DisposalApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DisposalApprovalService"));
    }

    @Bean
    public DisposalCalculationServiceGrpc.DisposalCalculationServiceStub assetsDisposalCalculationServiceAsync(ChannelRegistry channels) {
        return DisposalCalculationServiceGrpc.newStub(channels.lookup("acme.assets.v1.DisposalCalculationService"));
    }

    @Bean
    public DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking(ChannelRegistry channels) {
        return DisposalCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DisposalCalculationService"));
    }

    @Bean
    public DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking(ChannelRegistry channels) {
        return DisposalReportingServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DisposalReportingService"));
    }

    @Bean
    public DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync(ChannelRegistry channels) {
        return DisposalSyncServiceGrpc.newStub(channels.lookup("acme.assets.v1.DisposalSyncService"));
    }

    @Bean
    public DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking(ChannelRegistry channels) {
        return DisposalSyncServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DisposalSyncService"));
    }

    @Bean
    public DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking(ChannelRegistry channels) {
        return DisposalValidationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.DisposalValidationService"));
    }

    @Bean
    public DisposalValidationServiceGrpc.DisposalValidationServiceFutureStub assetsDisposalValidationServiceFuture(ChannelRegistry channels) {
        return DisposalValidationServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.DisposalValidationService"));
    }

    @Bean
    public FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceStub assetsFixedAssetApprovalServiceAsync(ChannelRegistry channels) {
        return FixedAssetApprovalServiceGrpc.newStub(channels.lookup("acme.assets.v1.FixedAssetApprovalService"));
    }

    @Bean
    public FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking(ChannelRegistry channels) {
        return FixedAssetApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetApprovalService"));
    }

    @Bean
    public FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking(ChannelRegistry channels) {
        return FixedAssetCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetCalculationService"));
    }

    @Bean
    public FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking(ChannelRegistry channels) {
        return FixedAssetLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetLifecycleService"));
    }

    @Bean
    public FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking(ChannelRegistry channels) {
        return FixedAssetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetQueryService"));
    }

    @Bean
    public FixedAssetQueryServiceGrpc.FixedAssetQueryServiceFutureStub assetsFixedAssetQueryServiceFuture(ChannelRegistry channels) {
        return FixedAssetQueryServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.FixedAssetQueryService"));
    }

    @Bean
    public FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync(ChannelRegistry channels) {
        return FixedAssetServiceGrpc.newStub(channels.lookup("acme.assets.v1.FixedAssetService"));
    }

    @Bean
    public FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking(ChannelRegistry channels) {
        return FixedAssetServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetService"));
    }

    @Bean
    public FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking(ChannelRegistry channels) {
        return FixedAssetValidationServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetValidationService"));
    }

    @Bean
    public FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceStub assetsFixedAssetWorkflowServiceAsync(ChannelRegistry channels) {
        return FixedAssetWorkflowServiceGrpc.newStub(channels.lookup("acme.assets.v1.FixedAssetWorkflowService"));
    }

    @Bean
    public FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceBlockingStub assetsFixedAssetWorkflowServiceBlocking(ChannelRegistry channels) {
        return FixedAssetWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.assets.v1.FixedAssetWorkflowService"));
    }

    @Bean
    public FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceFutureStub assetsFixedAssetWorkflowServiceFuture(ChannelRegistry channels) {
        return FixedAssetWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.assets.v1.FixedAssetWorkflowService"));
    }
}
