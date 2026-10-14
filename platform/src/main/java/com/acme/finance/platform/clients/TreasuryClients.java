package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.treasury.v1.BankTransferApprovalServiceGrpc;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferLifecycleServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastApprovalServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLifecycleServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastReportingServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the treasury contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TreasuryClients {
    @Bean
    public BankTransferApprovalServiceGrpc.BankTransferApprovalServiceBlockingStub treasuryBankTransferApprovalServiceBlocking(ChannelRegistry channels) {
        return BankTransferApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferApprovalService"));
    }

    @Bean
    public BankTransferCalculationServiceGrpc.BankTransferCalculationServiceStub treasuryBankTransferCalculationServiceAsync(ChannelRegistry channels) {
        return BankTransferCalculationServiceGrpc.newStub(channels.lookup("acme.treasury.v1.BankTransferCalculationService"));
    }

    @Bean
    public BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking(ChannelRegistry channels) {
        return BankTransferCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferCalculationService"));
    }

    @Bean
    public BankTransferLifecycleServiceGrpc.BankTransferLifecycleServiceBlockingStub treasuryBankTransferLifecycleServiceBlocking(ChannelRegistry channels) {
        return BankTransferLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferLifecycleService"));
    }

    @Bean
    public BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking(ChannelRegistry channels) {
        return BankTransferQueryServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferQueryService"));
    }

    @Bean
    public BankTransferQueryServiceGrpc.BankTransferQueryServiceFutureStub treasuryBankTransferQueryServiceFuture(ChannelRegistry channels) {
        return BankTransferQueryServiceGrpc.newFutureStub(channels.lookup("acme.treasury.v1.BankTransferQueryService"));
    }

    @Bean
    public BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking(ChannelRegistry channels) {
        return BankTransferReportingServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferReportingService"));
    }

    @Bean
    public BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking(ChannelRegistry channels) {
        return BankTransferValidationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferValidationService"));
    }

    @Bean
    public BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceStub treasuryBankTransferWorkflowServiceAsync(ChannelRegistry channels) {
        return BankTransferWorkflowServiceGrpc.newStub(channels.lookup("acme.treasury.v1.BankTransferWorkflowService"));
    }

    @Bean
    public BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceBlockingStub treasuryBankTransferWorkflowServiceBlocking(ChannelRegistry channels) {
        return BankTransferWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.BankTransferWorkflowService"));
    }

    @Bean
    public CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking(ChannelRegistry channels) {
        return CashPositionApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionApprovalService"));
    }

    @Bean
    public CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking(ChannelRegistry channels) {
        return CashPositionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionCalculationService"));
    }

    @Bean
    public CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceStub treasuryCashPositionLifecycleServiceAsync(ChannelRegistry channels) {
        return CashPositionLifecycleServiceGrpc.newStub(channels.lookup("acme.treasury.v1.CashPositionLifecycleService"));
    }

    @Bean
    public CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceBlockingStub treasuryCashPositionLifecycleServiceBlocking(ChannelRegistry channels) {
        return CashPositionLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionLifecycleService"));
    }

    @Bean
    public CashPositionQueryServiceGrpc.CashPositionQueryServiceStub treasuryCashPositionQueryServiceAsync(ChannelRegistry channels) {
        return CashPositionQueryServiceGrpc.newStub(channels.lookup("acme.treasury.v1.CashPositionQueryService"));
    }

    @Bean
    public CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking(ChannelRegistry channels) {
        return CashPositionQueryServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionQueryService"));
    }

    @Bean
    public CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync(ChannelRegistry channels) {
        return CashPositionServiceGrpc.newStub(channels.lookup("acme.treasury.v1.CashPositionService"));
    }

    @Bean
    public CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking(ChannelRegistry channels) {
        return CashPositionServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionService"));
    }

    @Bean
    public CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking(ChannelRegistry channels) {
        return CashPositionValidationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionValidationService"));
    }

    @Bean
    public CashPositionValidationServiceGrpc.CashPositionValidationServiceFutureStub treasuryCashPositionValidationServiceFuture(ChannelRegistry channels) {
        return CashPositionValidationServiceGrpc.newFutureStub(channels.lookup("acme.treasury.v1.CashPositionValidationService"));
    }

    @Bean
    public CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking(ChannelRegistry channels) {
        return CashPositionWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.CashPositionWorkflowService"));
    }

    @Bean
    public FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking(ChannelRegistry channels) {
        return FxHedgeApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.FxHedgeApprovalService"));
    }

    @Bean
    public FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking(ChannelRegistry channels) {
        return FxHedgeCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.FxHedgeCalculationService"));
    }

    @Bean
    public FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking(ChannelRegistry channels) {
        return FxHedgeReportingServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.FxHedgeReportingService"));
    }

    @Bean
    public FxHedgeSyncServiceGrpc.FxHedgeSyncServiceStub treasuryFxHedgeSyncServiceAsync(ChannelRegistry channels) {
        return FxHedgeSyncServiceGrpc.newStub(channels.lookup("acme.treasury.v1.FxHedgeSyncService"));
    }

    @Bean
    public FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking(ChannelRegistry channels) {
        return FxHedgeSyncServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.FxHedgeSyncService"));
    }

    @Bean
    public FxHedgeSyncServiceGrpc.FxHedgeSyncServiceFutureStub treasuryFxHedgeSyncServiceFuture(ChannelRegistry channels) {
        return FxHedgeSyncServiceGrpc.newFutureStub(channels.lookup("acme.treasury.v1.FxHedgeSyncService"));
    }

    @Bean
    public FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking(ChannelRegistry channels) {
        return FxHedgeValidationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.FxHedgeValidationService"));
    }

    @Bean
    public FxHedgeValidationServiceGrpc.FxHedgeValidationServiceFutureStub treasuryFxHedgeValidationServiceFuture(ChannelRegistry channels) {
        return FxHedgeValidationServiceGrpc.newFutureStub(channels.lookup("acme.treasury.v1.FxHedgeValidationService"));
    }

    @Bean
    public LiquidityForecastApprovalServiceGrpc.LiquidityForecastApprovalServiceBlockingStub treasuryLiquidityForecastApprovalServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastApprovalService"));
    }

    @Bean
    public LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastCalculationService"));
    }

    @Bean
    public LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync(ChannelRegistry channels) {
        return LiquidityForecastImportServiceGrpc.newStub(channels.lookup("acme.treasury.v1.LiquidityForecastImportService"));
    }

    @Bean
    public LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastImportServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastImportService"));
    }

    @Bean
    public LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceFutureStub treasuryLiquidityForecastImportServiceFuture(ChannelRegistry channels) {
        return LiquidityForecastImportServiceGrpc.newFutureStub(channels.lookup("acme.treasury.v1.LiquidityForecastImportService"));
    }

    @Bean
    public LiquidityForecastLifecycleServiceGrpc.LiquidityForecastLifecycleServiceBlockingStub treasuryLiquidityForecastLifecycleServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastLifecycleService"));
    }

    @Bean
    public LiquidityForecastReportingServiceGrpc.LiquidityForecastReportingServiceBlockingStub treasuryLiquidityForecastReportingServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastReportingServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastReportingService"));
    }

    @Bean
    public LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastSyncServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastSyncService"));
    }

    @Bean
    public LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceBlockingStub treasuryLiquidityForecastValidationServiceBlocking(ChannelRegistry channels) {
        return LiquidityForecastValidationServiceGrpc.newBlockingStub(channels.lookup("acme.treasury.v1.LiquidityForecastValidationService"));
    }
}
