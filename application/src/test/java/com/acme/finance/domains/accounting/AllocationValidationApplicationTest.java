package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.ExplainAccrualDecisionRequest;
import com.acme.accounting.v1.ExplainAccrualDecisionResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.GetAllocationRulesRequest;
import com.acme.accounting.v1.GetAllocationRulesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.LintAllocationsRequest;
import com.acme.accounting.v1.LintAllocationsResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeAccrualRequest;
import com.acme.accounting.v1.NormalizeAccrualResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.QuoteReconciliationRequest;
import com.acme.accounting.v1.QuoteReconciliationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ScoreAllocationRequest;
import com.acme.accounting.v1.ScoreAllocationResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyCondition;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.employees.v1.BrowseEmployeesRequest;
import com.acme.employees.v1.BrowseEmployeesResponse;
import com.acme.employees.v1.EmployeeQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.QuoteDeductionRequest;
import com.acme.payroll.v1.QuoteDeductionResponse;
import com.acme.recruiting.v1.CalculateJobOpeningRequest;
import com.acme.recruiting.v1.CalculateJobOpeningResponse;
import com.acme.recruiting.v1.InterviewRef;
import com.acme.recruiting.v1.JobOpening;
import com.acme.recruiting.v1.JobOpeningCalculationServiceGrpc;
import com.acme.recruiting.v1.JobOpeningLine;
import com.acme.search.v1.BatchCreateSearchIndexesRequest;
import com.acme.search.v1.BatchCreateSearchIndexesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AllocationValidationApplicationTest {
    @Test
    void scoreAllocationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AllocationValidationApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AccrualValidationServiceGrpc.newFutureStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                DunningValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                EmployeeQueryServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                JobOpeningCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(ScoreAllocationRequest.getDefaultInstance());
        var response = application.scoreAllocation(request);
        var sent0 = channel.request("acme.accounting.v1.ReconciliationCalculationService/RecalculateReconciliations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "created_at"),
                ProbeSupport.value(sent0, "reconciliation", "comments", "created_at"));
        var sent1 = channel.request("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/RecalculateReconciliations"), "reconciliation", "assignee", "user_id"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "manager", "user_id"));
        var sent2 = channel.request("acme.billing.v1.DunningValidationService/NormalizeDunning");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts"), "chart_of_accounts", "manager", "email"),
                ProbeSupport.value(sent2, "dunning", "requester", "email"));
        var sent3 = channel.request("acme.accounting.v1.AccrualValidationService/LintAccruals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/RecalculateReconciliations"), "reconciliation", "audit", "etag"),
                ProbeSupport.value(sent3, "accrual", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningValidationService/NormalizeDunning"), "dunning", "assignee", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningValidationService/NormalizeDunning"), "dunning", "audit", "created_at"),
                ProbeSupport.value(sent3, "accrual", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/RecalculateReconciliations"), "reconciliation", "audit", "updated_at"),
                ProbeSupport.value(response, "allocation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts"), "chart_of_accounts", "audit", "created_at"),
                ProbeSupport.value(response, "allocation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningValidationService/NormalizeDunning"), "dunning", "customer", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/LintAccruals"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation_id"),
                ProbeSupport.value(response, "allocation", "allocation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "priority"),
                ProbeSupport.value(response, "allocation", "priority"));
    }
}
