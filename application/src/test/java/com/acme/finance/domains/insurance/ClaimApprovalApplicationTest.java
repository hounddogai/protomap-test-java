package com.acme.finance.domains.insurance;

import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.ResolveFixedAssetsRequest;
import com.acme.assets.v1.ResolveFixedAssetsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ListArticlesRequest;
import com.acme.content.v1.ListArticlesResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.CountUnderwritingTotalsRequest;
import com.acme.insurance.v1.CountUnderwritingTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.DelegateClaimApprovalRequest;
import com.acme.insurance.v1.DelegateClaimApprovalResponse;
import com.acme.insurance.v1.DeletePolicyRequest;
import com.acme.insurance.v1.DeletePolicyResponse;
import com.acme.insurance.v1.EscalateClaimRequest;
import com.acme.insurance.v1.EscalateClaimResponse;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.ListPremiumReportsRequest;
import com.acme.insurance.v1.ListPremiumReportsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.RecallClaimRequest;
import com.acme.insurance.v1.RecallClaimResponse;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.loyalty.v1.ExplainLoyaltyMemberCalculationRequest;
import com.acme.loyalty.v1.ExplainLoyaltyMemberCalculationResponse;
import com.acme.loyalty.v1.LoyaltyMember;
import com.acme.loyalty.v1.LoyaltyMemberCalculationServiceGrpc;
import com.acme.loyalty.v1.RewardRef;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ClaimApprovalApplicationTest {
    @Test
    void delegateClaimApprovalPreservesPlannedFields() {
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
        var listener = new ClaimApprovalApplicationEvents(CoverageReportingServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ClaimApprovalApplication.ListPendingClaimsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ClaimApprovalApplication(
                FixedAssetQueryServiceGrpc.newFutureStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                events,
                PolicyServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                LoyaltyMemberCalculationServiceGrpc.newBlockingStub(channel),
                new ClaimApprovalRequestMapperImpl(),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(DelegateClaimApprovalRequest.getDefaultInstance());
        var response = application.delegateClaimApproval(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumReportingService/ListPremiumReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailSyncService/PullAuditTrails");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "assignee", "display_name"),
                ProbeSupport.value(sent1, "audit_trail", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "etag"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "created_by"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "created_at"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "created_at"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierService/BatchGetSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.insurance.v1.UnderwritingReportingService/CountUnderwritingTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/PullAuditTrails"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/PullAuditTrails"), "audit_trail", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/ListPremiumReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/ListPremiumReports"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/PullAuditTrails"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/PullAuditTrails"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingReportingService/CountUnderwritingTotals"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim_id"),
                ProbeSupport.value(response, "claim", "claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "subject"),
                ProbeSupport.value(response, "claim", "subject"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ClaimApprovalApplicationEvents(CoverageReportingServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListCoverageReportsResponse>();
        listener.on(new ClaimApprovalApplication.ListPendingClaimsStage0Requested(ProbeSupport.sample(ListCoverageReportsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
