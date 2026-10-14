package com.acme.finance.domains.budgeting;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.CalculateAdmissionRequest;
import com.acme.admissions.v1.CalculateAdmissionResponse;
import com.acme.admissions.v1.GateScanRef;
import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.ListAudiencesRequest;
import com.acme.audiences.v1.ListAudiencesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.ValidateBudgetLineRequest;
import com.acme.budgeting.v1.ValidateBudgetLineResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.maintenance.v1.QuoteWorkOrderRequest;
import com.acme.maintenance.v1.QuoteWorkOrderResponse;
import com.acme.maintenance.v1.RecalculateWorkOrdersRequest;
import com.acme.maintenance.v1.RecalculateWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.maintenance.v1.WorkOrderChannelOrigin;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BudgetLineValidationApplicationTest {
    @Test
    void validateBudgetLinePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLineValidationApplication(
                AdmissionCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                WorkOrderCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateBudgetLineRequest.getDefaultInstance());
        var response = application.validateBudgetLine(request);
        var sent0 = channel.request("acme.venues.v1.VenueValidationService/ScoreVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "audit", "created_by"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "compliance_case", "requester", "preferred_channel"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "audit", "updated_at"),
                ProbeSupport.value(sent2, "budget_line", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine"), "budget_line", "audit", "updated_at"),
                ProbeSupport.value(sent3, "budget_line", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "owner", "display_name"),
                ProbeSupport.value(response, "budget_line", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "address", "locality"),
                ProbeSupport.value(response, "budget_line", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase"), "breakdown", "total"),
                ProbeSupport.value(response, "budget_line", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine"), "budget_line", "billing_address", "country_code"),
                ProbeSupport.value(response, "budget_line", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "counterparty", "supplier_id"),
                ProbeSupport.value(response, "budget_line", "counterparty", "supplier_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "issued_at"),
                ProbeSupport.value(response, "budget_line", "issued_at"));
    }
}
