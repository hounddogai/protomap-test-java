package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.ExportHouseholdsRequest;
import com.acme.customers.v1.ExportHouseholdsResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateCoverageRequest;
import com.acme.insurance.v1.ActivateCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ExpireCoverageRequest;
import com.acme.insurance.v1.ExpireCoverageResponse;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.GetCoverageLifecycleRequest;
import com.acme.insurance.v1.GetCoverageLifecycleResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ReinstateCoverageRequest;
import com.acme.insurance.v1.ReinstateCoverageResponse;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.SuspendCoverageRequest;
import com.acme.insurance.v1.SuspendCoverageResponse;
import com.acme.insurance.v1.TerminateCoverageRequest;
import com.acme.insurance.v1.TerminateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import java.util.Optional;
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

class CoverageLifecycleApplicationTest {
    @Test
    void suspendCoveragePreservesPlannedFields() {
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
        var application = new CoverageLifecycleApplication(
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendCoverageRequest.getDefaultInstance());
        var response = application.suspendCoverage(request);
        var sent0 = channel.request("acme.insurance.v1.CoverageReportingService/GenerateCoverageReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(sent0, "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent1, "compliance_case", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "compliance_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "compliance_case", "requester", "email"));
        var sent2 = channel.request("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "assignee", "user_id"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "assignee", "display_name"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "audit", "etag"),
                ProbeSupport.value(sent2, "household", "children", "children", "audit", "etag"));
        var sent3 = channel.request("acme.ledger.v1.JournalEntryQueryService/FindJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "location", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "assignee", "user_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageReportingService/GenerateCoverageReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageReportingService/GenerateCoverageReport"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "name"),
                ProbeSupport.value(response, "coverage", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/FindJournalEntries"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/FindJournalEntries"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(response, "coverage", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
