package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePolicyRequest;
import com.acme.insurance.v1.ActivatePolicyResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.GetPolicyApprovalStatusRequest;
import com.acme.insurance.v1.GetPolicyApprovalStatusResponse;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.RejectPolicyRequest;
import com.acme.insurance.v1.RejectPolicyResponse;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ValidateRfqRequest;
import com.acme.procurement.v1.ValidateRfqResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PolicyApprovalApplicationTest {
    @Test
    void commentPolicyApprovalPreservesPlannedFields() {
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
        var listener = new PolicyApprovalApplicationEvents(RfqValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PolicyApprovalApplication.RecallPolicyStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PolicyApprovalApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newFutureStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                events,
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run,
                SupplierQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CommentPolicyApprovalRequest.getDefaultInstance());
        var response = application.commentPolicyApproval(request);
        var sent0 = channel.request("acme.procurement.v1.RfqValidationService/ValidateRfq");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "contact", "name"),
                ProbeSupport.value(sent0, "rfq", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "holder", "display_name"),
                ProbeSupport.value(sent0, "rfq", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "rfq", "assignee", "email"));
        var sent1 = channel.request("acme.insurance.v1.PremiumReportingService/GeneratePremiumReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "labels"),
                ProbeSupport.value(sent1, "details", "filters"));
        var sent2 = channel.request("acme.accounting.v1.AccrualQueryService/BrowseAccruals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/ValidateRfq"), "rfq", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "external_account_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.insurance.v1.UnderwritingReportingService/GenerateUnderwritingReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/GeneratePremiumReport"), "period"),
                ProbeSupport.value(sent3, "details", "period"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent3, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/ValidateRfq"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/ValidateRfq"), "rfq", "audit", "updated_by"),
                ProbeSupport.value(response, "policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/GeneratePremiumReport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/GeneratePremiumReport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualQueryService/BrowseAccruals"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingReportingService/GenerateUnderwritingReport"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy_id"),
                ProbeSupport.value(response, "policy", "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "balance"),
                ProbeSupport.value(response, "policy", "balance"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PolicyApprovalApplicationEvents(RfqValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PreviewRfqResponse>();
        listener.on(new PolicyApprovalApplication.RecallPolicyStage0Requested(ProbeSupport.sample(PreviewRfqRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
