package com.acme.finance.domains.expenses;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.RecallVarianceRequest;
import com.acme.budgeting.v1.RecallVarianceResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerEligibilityRequest;
import com.acme.customers.v1.CheckCustomerEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.employees.v1.WatchEmployeesRequest;
import com.acme.employees.v1.WatchEmployeesResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyRouteAction;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.ExplainExpenseReportDecisionRequest;
import com.acme.expenses.v1.ExplainExpenseReportDecisionResponse;
import com.acme.expenses.v1.ExportReceiptsRequest;
import com.acme.expenses.v1.ExportReceiptsResponse;
import com.acme.expenses.v1.FindExpenseReportsRequest;
import com.acme.expenses.v1.FindExpenseReportsResponse;
import com.acme.expenses.v1.GetExpensePolicySyncStateRequest;
import com.acme.expenses.v1.GetExpensePolicySyncStateResponse;
import com.acme.expenses.v1.LintExpensePoliciesRequest;
import com.acme.expenses.v1.LintExpensePoliciesResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.PullExpensePoliciesRequest;
import com.acme.expenses.v1.PullExpensePoliciesResponse;
import com.acme.expenses.v1.PushExpensePolicyRequest;
import com.acme.expenses.v1.PushExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ScoreReceiptRequest;
import com.acme.expenses.v1.ScoreReceiptResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.expenses.v1.SyncExpensePoliciesRequest;
import com.acme.expenses.v1.SyncExpensePoliciesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ExpensePolicySyncApplicationTest {
    @Test
    void pullExpensePoliciesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpensePolicySyncApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                EmployeeServiceGrpc.newBlockingStub(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                ReceiptQueryServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(PullExpensePoliciesRequest.getDefaultInstance());
        var response = application.pullExpensePolicies(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportQueryService/FindExpenseReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.notifications.v1.NotificationQueryService/FindNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.expenses.v1.ExpenseReportValidationService/ExplainExpenseReportDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "email"),
                ProbeSupport.value(sent2, "expense_report", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "discount", "amount"),
                ProbeSupport.value(sent2, "expense_report", "lines", "tax", "amount"));
        var sent3 = channel.request("acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportQueryService/FindExpenseReports"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportQueryService/FindExpenseReports"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/FindNotifications"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/FindNotifications"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/ExplainExpenseReportDecision"), "expense_report", "audit", "etag"),
                ProbeSupport.value(response, "expense_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy_id"),
                ProbeSupport.value(response, "expense_policy", "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "priority"),
                ProbeSupport.value(response, "expense_policy", "priority"));
    }

    @Test
    void streamExpensePolicyChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpensePolicySyncApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                EmployeeServiceGrpc.newBlockingStub(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                ReceiptQueryServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(StreamExpensePolicyChangesRequest.getDefaultInstance());
        var response = application.streamExpensePolicyChanges(request);
        var sent0 = channel.request("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "updated_by"),
                ProbeSupport.value(sent0, "customer", "audit", "updated_by"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportCalculationService/ExplainExpenseReportCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility"), "customer", "audit", "updated_by"),
                ProbeSupport.value(sent1, "expense_report", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/ExplainExpenseReportCalculation"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "priority"),
                ProbeSupport.value(response, "expense_policy", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "owner"),
                ProbeSupport.value(response, "expense_policy", "owner"));
        var endpoint = new ExpensePolicySyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamExpensePolicyChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamExpensePolicyChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamExpensePolicyChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
