package com.acme.finance.domains.insurance;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.billing.v1.SummarizeInvoicesRequest;
import com.acme.billing.v1.SummarizeInvoicesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.NormalizeHouseholdRequest;
import com.acme.customers.v1.NormalizeHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.CancelUnderwritingImportRequest;
import com.acme.insurance.v1.CancelUnderwritingImportResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.GetUnderwritingImportStatusRequest;
import com.acme.insurance.v1.GetUnderwritingImportStatusResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ratelimiting.v1.ExplainRateLimitCalculationRequest;
import com.acme.ratelimiting.v1.ExplainRateLimitCalculationResponse;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitCalculationServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingCalculationServiceGrpc;
import com.acme.workplaces.v1.SimulateBuildingRequest;
import com.acme.workplaces.v1.SimulateBuildingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class UnderwritingImportApplicationTest {
    @Test
    void importUnderwritingsPreservesPlannedFields() {
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
        var application = new UnderwritingImportApplication(
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new UnderwritingImportRequestMapperImpl(),
                RateLimitCalculationServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run,
                BuildingCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ImportUnderwritingsRequest.getDefaultInstance());
        var response = application.importUnderwritings(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyQueryService/BrowsePolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "history", "changed_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "customer", "claim_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.insurance.v1.ClaimValidationService/PreviewClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "etag"),
                ProbeSupport.value(sent1, "claim", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_by"),
                ProbeSupport.value(sent1, "claim", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/PreviewClaim"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/PreviewClaim"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        var endpoint = new UnderwritingImportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ImportUnderwritingsResponse> observer = new StreamObserver<>() {
            public void onNext(ImportUnderwritingsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.importUnderwritings(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void mapUnderwritingImportColumnsPreservesPlannedFields() {
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
        var application = new UnderwritingImportApplication(
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new UnderwritingImportRequestMapperImpl(),
                RateLimitCalculationServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run,
                BuildingCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(MapUnderwritingImportColumnsRequest.getDefaultInstance());
        var response = application.mapUnderwritingImportColumns(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyQueryService/ExportPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.ratelimiting.v1.RateLimitCalculationService/ExplainRateLimitCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "customer", "display_name"),
                ProbeSupport.value(sent1, "rate_limit", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "audit", "updated_by"),
                ProbeSupport.value(sent1, "rate_limit", "audit", "updated_by"));
        var sent2 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/SubscribeFiscalPeriodUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "comments", "author_email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "requester", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "requester", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/ExplainRateLimitCalculation"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/ExplainRateLimitCalculation"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting_id"),
                ProbeSupport.value(response, "underwriting", "underwriting_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "priority"),
                ProbeSupport.value(response, "underwriting", "priority"));
    }
}
