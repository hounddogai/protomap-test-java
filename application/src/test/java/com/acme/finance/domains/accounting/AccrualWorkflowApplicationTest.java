package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualCardSettlement;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AdvanceAccrualWorkflowRequest;
import com.acme.accounting.v1.AdvanceAccrualWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationRouteAction;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CancelAccrualWorkflowRequest;
import com.acme.accounting.v1.CancelAccrualWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsWorkflowServiceGrpc;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.GetAllocationRatesRequest;
import com.acme.accounting.v1.GetAllocationRatesResponse;
import com.acme.accounting.v1.InspectAccrualWorkflowRequest;
import com.acme.accounting.v1.InspectAccrualWorkflowResponse;
import com.acme.accounting.v1.ListAccrualWorkflowsRequest;
import com.acme.accounting.v1.ListAccrualWorkflowsResponse;
import com.acme.accounting.v1.PauseAccrualWorkflowRequest;
import com.acme.accounting.v1.PauseAccrualWorkflowResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.accounting.v1.ResumeAccrualWorkflowRequest;
import com.acme.accounting.v1.ResumeAccrualWorkflowResponse;
import com.acme.accounting.v1.RetryAccrualWorkflowRequest;
import com.acme.accounting.v1.RetryAccrualWorkflowResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.SignalAccrualWorkflowRequest;
import com.acme.accounting.v1.SignalAccrualWorkflowResponse;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.StartAccrualWorkflowRequest;
import com.acme.accounting.v1.StartAccrualWorkflowResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.CancelExpenseReportWorkflowRequest;
import com.acme.expenses.v1.CancelExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.orders.v1.EstimateOrderRequest;
import com.acme.orders.v1.EstimateOrderResponse;
import com.acme.orders.v1.Order;
import com.acme.orders.v1.OrderCalculationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.SubmitRfqRequest;
import com.acme.procurement.v1.SubmitRfqResponse;
import com.acme.suppliers.v1.ApproveSupplierContractRequest;
import com.acme.suppliers.v1.ApproveSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class AccrualWorkflowApplicationTest {
    @Test
    void signalAccrualWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AccrualWorkflowApplication(
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsQueryServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsWorkflowServiceGrpc.newFutureStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ExpenseReportWorkflowServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                OrderCalculationServiceGrpc.newBlockingStub(channel),
                RfqApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(SignalAccrualWorkflowRequest.getDefaultInstance());
        var response = application.signalAccrualWorkflow(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierContractApprovalService/ApproveSupplierContract");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "supplier_contract", "billing_address", "country_code"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportWorkflowService/AdvanceExpenseReportWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractApprovalService/ApproveSupplierContract"), "supplier_contract", "audit", "updated_at"),
                ProbeSupport.value(sent1, "expense_report", "audit", "updated_at"));
        var sent2 = channel.request("acme.accounting.v1.AllocationCalculationService/RecalculateAllocations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/AdvanceExpenseReportWorkflow"), "expense_report", "counterparty", "email"),
                ProbeSupport.value(sent2, "allocation", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "details", "destination", "postal_code"));
        var sent3 = channel.request("acme.accounting.v1.AllocationCalculationService/CalculateAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/RecalculateAllocations"), "allocation", "route", "weight"),
                ProbeSupport.value(sent3, "allocation", "route", "weight"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/AdvanceExpenseReportWorkflow"), "expense_report", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "allocation", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractApprovalService/ApproveSupplierContract"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractApprovalService/ApproveSupplierContract"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/AdvanceExpenseReportWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/AdvanceExpenseReportWorkflow"), "expense_report", "audit", "created_at"),
                ProbeSupport.value(response, "accrual", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/RecalculateAllocations"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/RecalculateAllocations"), "breakdown", "total"),
                ProbeSupport.value(response, "accrual", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/CalculateAllocation"), "allocation", "audit", "created_by"),
                ProbeSupport.value(response, "accrual", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual_id"),
                ProbeSupport.value(response, "accrual", "accrual_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "accrual", "notes"),
                ProbeSupport.value(response, "accrual", "notes"));
    }
}
