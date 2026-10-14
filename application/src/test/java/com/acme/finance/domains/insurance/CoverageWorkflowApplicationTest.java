package com.acme.finance.domains.insurance;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermBlockAction;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CountHouseholdsRequest;
import com.acme.customers.v1.CountHouseholdsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvanceClaimWorkflowRequest;
import com.acme.insurance.v1.AdvanceClaimWorkflowResponse;
import com.acme.insurance.v1.AdvanceCoverageWorkflowRequest;
import com.acme.insurance.v1.AdvanceCoverageWorkflowResponse;
import com.acme.insurance.v1.CancelCoverageWorkflowRequest;
import com.acme.insurance.v1.CancelCoverageWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.InspectCoverageWorkflowRequest;
import com.acme.insurance.v1.InspectCoverageWorkflowResponse;
import com.acme.insurance.v1.ListCoverageWorkflowsRequest;
import com.acme.insurance.v1.ListCoverageWorkflowsResponse;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.insurance.v1.RetryCoverageWorkflowRequest;
import com.acme.insurance.v1.RetryCoverageWorkflowResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.insurance.v1.StartCoverageWorkflowRequest;
import com.acme.insurance.v1.StartCoverageWorkflowResponse;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.subscriptions.v1.ListSubscriptionsRequest;
import com.acme.subscriptions.v1.ListSubscriptionsResponse;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class CoverageWorkflowApplicationTest {
    @Test
    void startCoverageWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InsuranceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveClaim(String tenant, Claim value) {}
            public void saveCoverage(String tenant, Coverage value) {}
            public void savePolicy(String tenant, Policy value) {}
            public void savePremium(String tenant, Premium value) {}
            public void saveUnderwriting(String tenant, Underwriting value) {}
        };
        var listener = new CoverageWorkflowApplicationEvents(ConsentRecordCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CoverageWorkflowApplication.CancelCoverageWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CoverageWorkflowApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                events,
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                CoverageLifecycleServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                new CoverageWorkflowRequestMapperImpl(),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SubscriptionServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartCoverageWorkflowRequest.getDefaultInstance());
        var response = application.startCoverageWorkflow(request);
        var sent0 = channel.request("acme.insurance.v1.ClaimApprovalService/ListPendingClaims");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "parameters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.insurance.v1.CoverageLifecycleService/ScheduleCoverageExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "discount", "amount"),
                ProbeSupport.value(sent1, "coverage", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "email"),
                ProbeSupport.value(sent1, "coverage", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "created_at"),
                ProbeSupport.value(sent1, "coverage", "audit", "created_at"));
        var sent2 = channel.request("acme.insurance.v1.PolicyApprovalService/SubmitPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "created_by"),
                ProbeSupport.value(sent2, "policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "updated_by"),
                ProbeSupport.value(sent2, "policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent2, "policy", "contact", "name"));
        var sent3 = channel.request("acme.insurance.v1.ClaimWorkflowService/AdvanceClaimWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/SubmitPolicy"), "policy", "contact", "phone"),
                ProbeSupport.value(sent3, "claim", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent3, "claim", "customer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/SubmitPolicy"), "policy", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimApprovalService/ListPendingClaims"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimApprovalService/ListPendingClaims"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageLifecycleService/ScheduleCoverageExpiry"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimWorkflowService/AdvanceClaimWorkflow"), "claim", "audit", "updated_at"),
                ProbeSupport.value(response, "coverage", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(response, "coverage", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "description"),
                ProbeSupport.value(response, "coverage", "description"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CoverageWorkflowApplicationEvents(ConsentRecordCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuoteConsentRecordResponse>();
        listener.on(new CoverageWorkflowApplication.CancelCoverageWorkflowStage0Requested(ProbeSupport.sample(QuoteConsentRecordRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
