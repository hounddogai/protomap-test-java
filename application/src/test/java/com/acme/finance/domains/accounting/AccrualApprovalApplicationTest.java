package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.ApproveAccrualRequest;
import com.acme.accounting.v1.ApproveAccrualResponse;
import com.acme.accounting.v1.BrowseChartOfAccountsesRequest;
import com.acme.accounting.v1.BrowseChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.CommentAccrualApprovalRequest;
import com.acme.accounting.v1.CommentAccrualApprovalResponse;
import com.acme.accounting.v1.CountAccrualsRequest;
import com.acme.accounting.v1.CountAccrualsResponse;
import com.acme.accounting.v1.DelegateAccrualApprovalRequest;
import com.acme.accounting.v1.DelegateAccrualApprovalResponse;
import com.acme.accounting.v1.EscalateAccrualRequest;
import com.acme.accounting.v1.EscalateAccrualResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExportChartOfAccountsesRequest;
import com.acme.accounting.v1.ExportChartOfAccountsesResponse;
import com.acme.accounting.v1.FindAccrualsRequest;
import com.acme.accounting.v1.FindAccrualsResponse;
import com.acme.accounting.v1.GetAccrualApprovalStatusRequest;
import com.acme.accounting.v1.GetAccrualApprovalStatusResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.SubmitAccrualRequest;
import com.acme.accounting.v1.SubmitAccrualResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverCalculationServiceGrpc;
import com.acme.drivers.v1.QuoteDriverRequest;
import com.acme.drivers.v1.QuoteDriverResponse;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.GetUserRulesRequest;
import com.acme.identity.v1.GetUserRulesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
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
import org.springframework.stereotype.Service;

class AccrualApprovalApplicationTest {
    @Test
    void commentAccrualApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AccrualApprovalApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsQueryServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineWorkflowServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                DriverCalculationServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CommentAccrualApprovalRequest.getDefaultInstance());
        var response = application.commentAccrualApproval(request);
        var sent0 = channel.request("acme.budgeting.v1.ForecastReportingService/GenerateForecastReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.accounting.v1.AccrualQueryService/CountAccruals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "issued_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "counterparty", "allocation_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.drivers.v1.DriverCalculationService/QuoteDriver");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "driver", "relationship_manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "audit", "updated_at"),
                ProbeSupport.value(sent3, "driver", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "locality"),
                ProbeSupport.value(sent3, "driver", "primary_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "lines"),
                ProbeSupport.value(sent3, "driver", "primary_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/GenerateForecastReport"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualQueryService/CountAccruals"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.drivers.v1.DriverCalculationService/QuoteDriver"), "driver", "audit", "updated_by"),
                ProbeSupport.value(response, "accrual", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual_id"),
                ProbeSupport.value(response, "accrual", "accrual_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
