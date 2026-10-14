package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.LintAllocationsRequest;
import com.acme.accounting.v1.LintAllocationsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SummarizeCustomersRequest;
import com.acme.customers.v1.SummarizeCustomersResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateClaimRequest;
import com.acme.insurance.v1.ActivateClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ExpireClaimRequest;
import com.acme.insurance.v1.ExpireClaimResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.RecalculateUnderwritingsRequest;
import com.acme.insurance.v1.RecalculateUnderwritingsResponse;
import com.acme.insurance.v1.ReinstateClaimRequest;
import com.acme.insurance.v1.ReinstateClaimResponse;
import com.acme.insurance.v1.RenewClaimRequest;
import com.acme.insurance.v1.RenewClaimResponse;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.StreamCoverageMetricsRequest;
import com.acme.insurance.v1.StreamCoverageMetricsResponse;
import com.acme.insurance.v1.SuspendClaimRequest;
import com.acme.insurance.v1.SuspendClaimResponse;
import com.acme.insurance.v1.TerminateClaimRequest;
import com.acme.insurance.v1.TerminateClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionRequest;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionResponse;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
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

class ClaimLifecycleApplicationTest {
    @Test
    void suspendClaimPreservesPlannedFields() {
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
        var listener = new ClaimLifecycleApplicationEvents(CustomerQueryServiceGrpc.newStub(channel), EInvoiceValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ClaimLifecycleApplication.SuspendClaimStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof ClaimLifecycleApplication.RenewClaimStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ClaimLifecycleApplication(
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                events,
                ClaimCalculationServiceGrpc.newFutureStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceValidationServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendClaimRequest.getDefaultInstance());
        var response = application.suspendClaim(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent0, "customer_segment", "owner", "email"));
        var sent1 = channel.request("acme.invoicing.v1.EInvoiceValidationService/ExplainEInvoiceDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment"), "customer_segment", "audit", "created_by"),
                ProbeSupport.value(sent1, "e_invoice", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent1, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment"), "customer_segment", "audit", "etag"),
                ProbeSupport.value(sent1, "e_invoice", "audit", "etag"));
        var sent2 = channel.request("acme.insurance.v1.CoverageReportingService/StreamCoverageMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent2, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ExplainEInvoiceDecision"), "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.assets.v1.AssetTransferValidationService/ValidateAssetTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ExplainEInvoiceDecision"), "e_invoice", "audit", "created_by"),
                ProbeSupport.value(sent3, "asset_transfer", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ExplainEInvoiceDecision"), "e_invoice", "audit", "etag"),
                ProbeSupport.value(sent3, "asset_transfer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ExplainEInvoiceDecision"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferValidationService/ValidateAssetTransfer"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim_id"),
                ProbeSupport.value(response, "claim", "claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ClaimLifecycleApplicationEvents(CustomerQueryServiceGrpc.newStub(channel), EInvoiceValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainEInvoiceDecisionResponse>();
        listener.on(new ClaimLifecycleApplication.SuspendClaimStage1Requested(ProbeSupport.sample(ExplainEInvoiceDecisionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<SummarizeCustomersResponse>();
        listener.on(new ClaimLifecycleApplication.RenewClaimStage0Requested(ProbeSupport.sample(SummarizeCustomersRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
