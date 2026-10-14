package com.acme.finance.domains.expenses;

import com.acme.catalog.v1.FindProductsRequest;
import com.acme.catalog.v1.FindProductsResponse;
import com.acme.catalog.v1.ProductQueryServiceGrpc;
import com.acme.chat.v1.CalculateChatRoomRequest;
import com.acme.chat.v1.CalculateChatRoomResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomCalculationServiceGrpc;
import com.acme.chat.v1.ChatRoomComponent;
import com.acme.chat.v1.QuoteChatRoomRequest;
import com.acme.chat.v1.QuoteChatRoomResponse;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.employees.v1.EmployeeRef;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.DelegateExpenseReportApprovalRequest;
import com.acme.expenses.v1.DelegateExpenseReportApprovalResponse;
import com.acme.expenses.v1.EscalateExpenseReportRequest;
import com.acme.expenses.v1.EscalateExpenseReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListPendingExpenseReportsRequest;
import com.acme.expenses.v1.ListPendingExpenseReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RejectExpenseReportRequest;
import com.acme.expenses.v1.RejectExpenseReportResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.expenses.v1.SubmitExpenseReportRequest;
import com.acme.expenses.v1.SubmitExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CountLedgerAccountsRequest;
import com.acme.ledger.v1.CountLedgerAccountsResponse;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.maintenance.v1.CompareWorkOrderOptionsRequest;
import com.acme.maintenance.v1.CompareWorkOrderOptionsResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.media.v1.ExplainMediaAssetCalculationRequest;
import com.acme.media.v1.ExplainMediaAssetCalculationResponse;
import com.acme.media.v1.MediaAsset;
import com.acme.media.v1.MediaAssetCalculationServiceGrpc;
import com.acme.timekeeping.v1.CreateTimesheetRequest;
import com.acme.timekeeping.v1.CreateTimesheetResponse;
import com.acme.timekeeping.v1.Timesheet;
import com.acme.timekeeping.v1.TimesheetServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ExpenseReportApprovalApplicationTest {
    @Test
    void recallExpenseReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ExpenseReportApprovalApplicationEvents(LedgerAccountQueryServiceGrpc.newStub(channel), MediaAssetCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ExpenseReportApprovalApplication.EscalateExpenseReportStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ExpenseReportApprovalApplication.RecallExpenseReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ExpenseReportApprovalApplication(
                ProductQueryServiceGrpc.newFutureStub(channel),
                ChannelRegistry.fixed(channel),
                ChatRoomCalculationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimReportingServiceGrpc.newFutureStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                WorkOrderCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                TimesheetServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallExpenseReportRequest.getDefaultInstance());
        var response = application.recallExpenseReport(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "metadata"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "counterparty", "customer_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.media.v1.MediaAssetCalculationService/ExplainMediaAssetCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "media_asset", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "locality"),
                ProbeSupport.value(sent1, "media_asset", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent1, "media_asset", "owner", "user_id"));
        var sent2 = channel.request("acme.customers.v1.PreferenceCalculationService/QuotePreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "lines"),
                ProbeSupport.value(sent2, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "audit", "updated_by"),
                ProbeSupport.value(sent2, "preference", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        var sent3 = channel.request("acme.expenses.v1.ExpensePolicyValidationService/ScoreExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent3, "expense_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.media.v1.MediaAssetCalculationService/ExplainMediaAssetCalculation"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.media.v1.MediaAssetCalculationService/ExplainMediaAssetCalculation"), "media_asset", "address", "recipient"),
                ProbeSupport.value(response, "expense_report", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/QuotePreference"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/QuotePreference"), "preference", "audit", "updated_by"),
                ProbeSupport.value(response, "expense_report", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/ScoreExpensePolicy"), "expense_policy", "discount", "amount"),
                ProbeSupport.value(response, "expense_report", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report_id"),
                ProbeSupport.value(response, "expense_report", "expense_report_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "issued_at"),
                ProbeSupport.value(response, "expense_report", "issued_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ExpenseReportApprovalApplicationEvents(LedgerAccountQueryServiceGrpc.newStub(channel), MediaAssetCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<FindLedgerAccountsResponse>();
        listener.on(new ExpenseReportApprovalApplication.EscalateExpenseReportStage0Requested(ProbeSupport.sample(FindLedgerAccountsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ExplainMediaAssetCalculationResponse>();
        listener.on(new ExpenseReportApprovalApplication.RecallExpenseReportStage1Requested(ProbeSupport.sample(ExplainMediaAssetCalculationRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
