package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionRequest;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.maintenance.v1.EstimateWorkOrderRequest;
import com.acme.maintenance.v1.EstimateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionRequest;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.RecalculatePrescriptionsRequest;
import com.acme.prescriptions.v1.RecalculatePrescriptionsResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CountBankTransferTotalsRequest;
import com.acme.treasury.v1.CountBankTransferTotalsResponse;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.DeleteBankTransferReportRequest;
import com.acme.treasury.v1.DeleteBankTransferReportResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FetchBankTransferReportRequest;
import com.acme.treasury.v1.FetchBankTransferReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GenerateBankTransferReportRequest;
import com.acme.treasury.v1.GenerateBankTransferReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.ListBankTransferReportsRequest;
import com.acme.treasury.v1.ListBankTransferReportsResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.ReconcileFxHedgesRequest;
import com.acme.treasury.v1.ReconcileFxHedgesResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.ShareBankTransferReportRequest;
import com.acme.treasury.v1.ShareBankTransferReportResponse;
import com.acme.treasury.v1.StreamBankTransferMetricsRequest;
import com.acme.treasury.v1.StreamBankTransferMetricsResponse;
import com.acme.treasury.v1.StreamFxHedgeChangesRequest;
import com.acme.treasury.v1.StreamFxHedgeChangesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BankTransferReportingApplicationTest {
    @Test
    void streamBankTransferMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferReportingApplication(
                ChannelRegistry.fixed(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newFutureStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                WorkOrderCalculationServiceGrpc.newBlockingStub(channel),
                new BankTransferReportingRequestMapperImpl(),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                PrescriptionCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                CashPositionQueryServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamBankTransferMetricsRequest.getDefaultInstance());
        var response = application.streamBankTransferMetrics(request);
        var sent0 = channel.request("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "bank_transfer", "counterparty", "email"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionQueryService/CountCashPositions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent1, "filter", "query"));
        var sent2 = channel.request("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer"), "bank_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "liquidity_forecast", "billing_address", "postal_code"));
        var sent3 = channel.request("acme.compliance.v1.AttestationApprovalService/ApproveAttestation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts"), "liquidity_forecast", "billing_address", "postal_code"),
                ProbeSupport.value(sent3, "attestation", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer"), "bank_transfer", "audit", "updated_at"),
                ProbeSupport.value(response, "bank_transfer", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer"), "bank_transfer", "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionQueryService/CountCashPositions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts"), "liquidity_forecast", "number"),
                ProbeSupport.value(response, "bank_transfer", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts"), "liquidity_forecast", "billing_address", "region"),
                ProbeSupport.value(response, "bank_transfer", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationApprovalService/ApproveAttestation"), "attestation", "billing_address", "postal_code"),
                ProbeSupport.value(response, "bank_transfer", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationApprovalService/ApproveAttestation"), "attestation", "due_at"),
                ProbeSupport.value(response, "bank_transfer", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "bank_transfer", "counterparty", "email"));
        var endpoint = new BankTransferReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamBankTransferMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamBankTransferMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamBankTransferMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
