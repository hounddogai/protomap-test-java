package com.acme.finance.domains.insurance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.CreateComplianceCaseRequest;
import com.acme.compliance.v1.CreateComplianceCaseResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordMeasurement;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.AdvanceClaimWorkflowRequest;
import com.acme.insurance.v1.AdvanceClaimWorkflowResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.CancelClaimWorkflowRequest;
import com.acme.insurance.v1.CancelClaimWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.GetPolicyApprovalStatusRequest;
import com.acme.insurance.v1.GetPolicyApprovalStatusResponse;
import com.acme.insurance.v1.GetPolicyRequest;
import com.acme.insurance.v1.GetPolicyResponse;
import com.acme.insurance.v1.InspectClaimWorkflowRequest;
import com.acme.insurance.v1.InspectClaimWorkflowResponse;
import com.acme.insurance.v1.ListClaimWorkflowsRequest;
import com.acme.insurance.v1.ListClaimWorkflowsResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.PauseClaimWorkflowRequest;
import com.acme.insurance.v1.PauseClaimWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.RejectPolicyRequest;
import com.acme.insurance.v1.RejectPolicyResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.RetryClaimWorkflowRequest;
import com.acme.insurance.v1.RetryClaimWorkflowResponse;
import com.acme.insurance.v1.SignalClaimWorkflowRequest;
import com.acme.insurance.v1.SignalClaimWorkflowResponse;
import com.acme.insurance.v1.StartClaimWorkflowRequest;
import com.acme.insurance.v1.StartClaimWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteServiceGrpc;
import com.acme.routing.v1.UpdateRouteRequest;
import com.acme.routing.v1.UpdateRouteResponse;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskSla;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
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
import org.springframework.stereotype.Service;

class ClaimWorkflowApplicationTest {
    @Test
    void pauseClaimWorkflowPreservesPlannedFields() {
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
        var application = new ClaimWorkflowApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                PolicyServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newFutureStub(channel),
                repository,
                RouteServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PauseClaimWorkflowRequest.getDefaultInstance());
        var response = application.pauseClaimWorkflow(request);
        var sent0 = channel.request("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "assignee", "display_name"),
                ProbeSupport.value(sent0, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "created_at"),
                ProbeSupport.value(sent0, "consent_record", "measurements", "observed_at"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierRiskValidationService/ExplainSupplierRiskDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "supplier_risk", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "sla", "due_at"),
                ProbeSupport.value(sent1, "supplier_risk", "sla", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "requester", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "created_at"),
                ProbeSupport.value(sent1, "supplier_risk", "audit", "created_at"));
        var sent2 = channel.request("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "created_by"),
                ProbeSupport.value(sent2, "policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "etag"),
                ProbeSupport.value(sent2, "policy", "audit", "etag"));
        var sent3 = channel.request("acme.customers.v1.PreferenceValidationService/PreviewPreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "comments", "created_at"),
                ProbeSupport.value(sent3, "preference", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "comments", "author_email"),
                ProbeSupport.value(sent3, "preference", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "assignee", "display_name"),
                ProbeSupport.value(sent3, "preference", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/ExplainSupplierRiskDecision"), "supplier_risk", "requester", "preferred_channel"),
                ProbeSupport.value(response, "claim", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow"), "policy", "audit", "updated_by"),
                ProbeSupport.value(response, "claim", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim_id"),
                ProbeSupport.value(response, "claim", "claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "custom_fields"),
                ProbeSupport.value(response, "claim", "custom_fields"));
    }
}
