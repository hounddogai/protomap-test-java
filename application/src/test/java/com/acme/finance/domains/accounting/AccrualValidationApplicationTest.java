package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainAccrualDecisionRequest;
import com.acme.accounting.v1.ExplainAccrualDecisionResponse;
import com.acme.accounting.v1.GetAccrualRulesRequest;
import com.acme.accounting.v1.GetAccrualRulesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.NormalizeAccrualRequest;
import com.acme.accounting.v1.NormalizeAccrualResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ScoreAccrualRequest;
import com.acme.accounting.v1.ScoreAccrualResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.BrowseExpenseReportsRequest;
import com.acme.expenses.v1.BrowseExpenseReportsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ValidateCollateralRequest;
import com.acme.lending.v1.ValidateCollateralResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AccrualValidationApplicationTest {
    @Test
    void normalizeAccrualPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new AccrualValidationApplicationEvents(FiscalPeriodNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AccrualValidationApplication.ExplainAccrualDecisionStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AccrualValidationApplication(
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserValidationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                repository,
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(NormalizeAccrualRequest.getDefaultInstance());
        var response = application.normalizeAccrual(request);
        var sent0 = channel.request("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent0, "fixed_asset", "owner", "email"));
        var sent1 = channel.request("acme.audit.v1.AuditEventQueryService/CountAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "specs"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.lending.v1.CollateralValidationService/ValidateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "collateral", "address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "dimensions", "height"),
                ProbeSupport.value(sent2, "collateral", "dimensions", "height"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "audit", "updated_by"),
                ProbeSupport.value(sent2, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "owner", "email"),
                ProbeSupport.value(sent2, "collateral", "owner", "email"));
        var sent3 = channel.request("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "address", "recipient"),
                ProbeSupport.value(sent3, "fx_hedge", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "address", "region"),
                ProbeSupport.value(response, "accrual", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventQueryService/CountAuditEvents"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/ValidateCollateral"), "collateral", "audit", "updated_at"),
                ProbeSupport.value(response, "accrual", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge"), "fx_hedge", "billing_address", "lines"),
                ProbeSupport.value(response, "accrual", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual_id"),
                ProbeSupport.value(response, "accrual", "accrual_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "total"),
                ProbeSupport.value(response, "accrual", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AccrualValidationApplicationEvents(FiscalPeriodNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListFiscalPeriodSubscriptionsResponse>();
        listener.on(new AccrualValidationApplication.ExplainAccrualDecisionStage2Requested(ProbeSupport.sample(ListFiscalPeriodSubscriptionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
