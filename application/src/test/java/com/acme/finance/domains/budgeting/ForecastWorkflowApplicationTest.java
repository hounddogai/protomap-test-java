package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.budgeting.v1.AdvanceForecastWorkflowRequest;
import com.acme.budgeting.v1.AdvanceForecastWorkflowResponse;
import com.acme.budgeting.v1.ApproveForecastRequest;
import com.acme.budgeting.v1.ApproveForecastResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CancelForecastWorkflowRequest;
import com.acme.budgeting.v1.CancelForecastWorkflowResponse;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.InspectForecastWorkflowRequest;
import com.acme.budgeting.v1.InspectForecastWorkflowResponse;
import com.acme.budgeting.v1.ListForecastWorkflowsRequest;
import com.acme.budgeting.v1.ListForecastWorkflowsResponse;
import com.acme.budgeting.v1.PauseForecastWorkflowRequest;
import com.acme.budgeting.v1.PauseForecastWorkflowResponse;
import com.acme.budgeting.v1.PeekBudgetRequest;
import com.acme.budgeting.v1.PeekBudgetResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.ResumeForecastWorkflowRequest;
import com.acme.budgeting.v1.ResumeForecastWorkflowResponse;
import com.acme.budgeting.v1.RetryForecastWorkflowRequest;
import com.acme.budgeting.v1.RetryForecastWorkflowResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.StartForecastWorkflowRequest;
import com.acme.budgeting.v1.StartForecastWorkflowResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ForecastWorkflowApplicationTest {
    @Test
    void cancelForecastWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new ForecastWorkflowApplicationEvents(BudgetApprovalServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ForecastWorkflowApplication.CancelForecastWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ForecastWorkflowApplication(
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                ForecastApprovalServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                PaymentPromiseApprovalServiceGrpc.newFutureStub(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                CoverageLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CancelForecastWorkflowRequest.getDefaultInstance());
        var response = application.cancelForecastWorkflow(request);
        var sent0 = channel.request("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "recipient", "preferred_channel"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetApprovalService/EscalateBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "audit", "updated_by"),
                ProbeSupport.value(sent1, "budget", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "owner", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "audit", "created_by"),
                ProbeSupport.value(sent1, "budget", "audit", "created_by"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/CheckSupplierRatingEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "billing_address", "locality"),
                ProbeSupport.value(sent2, "supplier_rating", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "owner", "display_name"),
                ProbeSupport.value(sent2, "supplier_rating", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "card", "token"),
                ProbeSupport.value(sent2, "supplier_rating", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/EscalateBudget"), "budget", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "contact", "phone"));
        var sent3 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "audit", "created_by"),
                ProbeSupport.value(sent3, "feature_flag", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "audit", "updated_by"),
                ProbeSupport.value(sent3, "feature_flag", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/CheckSupplierRatingEligibility"), "supplier_rating", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "feature_flag", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/EscalateBudget"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/EscalateBudget"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/CheckSupplierRatingEligibility"), "supplier_rating", "billing_address", "country_code"),
                ProbeSupport.value(response, "forecast", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/CheckSupplierRatingEligibility"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(response, "forecast", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast_id"),
                ProbeSupport.value(response, "forecast", "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "metadata"),
                ProbeSupport.value(response, "forecast", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ForecastWorkflowApplicationEvents(BudgetApprovalServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EscalateBudgetResponse>();
        listener.on(new ForecastWorkflowApplication.CancelForecastWorkflowStage1Requested(ProbeSupport.sample(EscalateBudgetRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
