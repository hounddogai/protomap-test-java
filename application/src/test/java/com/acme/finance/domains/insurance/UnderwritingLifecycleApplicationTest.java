package com.acme.finance.domains.insurance;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.ResolveAnalyticsEventsRequest;
import com.acme.analytics.v1.ResolveAnalyticsEventsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateUnderwritingRequest;
import com.acme.insurance.v1.ActivateUnderwritingResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.CountUnderwritingTotalsRequest;
import com.acme.insurance.v1.CountUnderwritingTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CreatePolicyRequest;
import com.acme.insurance.v1.CreatePolicyResponse;
import com.acme.insurance.v1.ExpireUnderwritingRequest;
import com.acme.insurance.v1.ExpireUnderwritingResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.GetUnderwritingLifecycleRequest;
import com.acme.insurance.v1.GetUnderwritingLifecycleResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.ReinstateUnderwritingRequest;
import com.acme.insurance.v1.ReinstateUnderwritingResponse;
import com.acme.insurance.v1.RenewUnderwritingRequest;
import com.acme.insurance.v1.RenewUnderwritingResponse;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryRequest;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.SuspendUnderwritingRequest;
import com.acme.insurance.v1.SuspendUnderwritingResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.TerminateUnderwritingRequest;
import com.acme.insurance.v1.TerminateUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class UnderwritingLifecycleApplicationTest {
    @Test
    void scheduleUnderwritingExpiryPreservesPlannedFields() {
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
        var application = new UnderwritingLifecycleApplication(
                AnalyticsEventQueryServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                PolicyServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newFutureStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScheduleUnderwritingExpiryRequest.getDefaultInstance());
        var response = application.scheduleUnderwritingExpiry(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyApprovalService/ApprovePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "audit", "updated_at"),
                ProbeSupport.value(sent0, "policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "audit", "created_by"),
                ProbeSupport.value(sent0, "policy", "audit", "created_by"));
        var sent1 = channel.request("acme.analytics.v1.AnalyticsEventQueryService/ResolveAnalyticsEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/ApprovePolicy"), "policy", "audit", "created_at"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "audit", "etag"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "etag"));
        var sent3 = channel.request("acme.insurance.v1.PremiumSyncService/SyncPremiums");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/ApprovePolicy"), "policy", "audit", "updated_by"),
                ProbeSupport.value(sent3, "premium", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent3, "premium", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "requester", "email"),
                ProbeSupport.value(sent3, "premium", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/ApprovePolicy"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.analytics.v1.AnalyticsEventQueryService/ResolveAnalyticsEvents"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "audit", "updated_by"),
                ProbeSupport.value(response, "underwriting", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumSyncService/SyncPremiums"), "premium", "audit", "etag"),
                ProbeSupport.value(response, "underwriting", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting_id"),
                ProbeSupport.value(response, "underwriting", "underwriting_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "assignee"),
                ProbeSupport.value(response, "underwriting", "assignee"));
    }
}
