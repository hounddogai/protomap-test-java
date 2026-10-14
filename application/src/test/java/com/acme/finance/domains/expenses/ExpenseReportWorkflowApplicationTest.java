package com.acme.finance.domains.expenses;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.BrowseBenefitPlansRequest;
import com.acme.benefits.v1.BrowseBenefitPlansResponse;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ApproveReceiptRequest;
import com.acme.expenses.v1.ApproveReceiptResponse;
import com.acme.expenses.v1.CancelExpenseReportWorkflowRequest;
import com.acme.expenses.v1.CancelExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.InspectExpenseReportWorkflowRequest;
import com.acme.expenses.v1.InspectExpenseReportWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.PauseExpenseReportWorkflowRequest;
import com.acme.expenses.v1.PauseExpenseReportWorkflowResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RetryExpenseReportWorkflowRequest;
import com.acme.expenses.v1.RetryExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SignalExpenseReportWorkflowRequest;
import com.acme.expenses.v1.SignalExpenseReportWorkflowResponse;
import com.acme.expenses.v1.StartExpenseReportWorkflowRequest;
import com.acme.expenses.v1.StartExpenseReportWorkflowResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.SignalPolicyWorkflowRequest;
import com.acme.insurance.v1.SignalPolicyWorkflowResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationComment;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ExpenseReportWorkflowApplicationTest {
    @Test
    void startExpenseReportWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportWorkflowApplication(
                ChartOfAccountsServiceGrpc.newStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newFutureStub(channel),
                BenefitPlanQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ReceiptApprovalServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newFutureStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StartExpenseReportWorkflowRequest.getDefaultInstance());
        var response = application.startExpenseReportWorkflow(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineReportingService/ListBudgetLineReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent1, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "details", "recipient", "phone"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyValidationService/NormalizeRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "audit", "created_by"),
                ProbeSupport.value(sent2, "retention_policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "audit", "updated_at"),
                ProbeSupport.value(sent2, "retention_policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "retention_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        var sent3 = channel.request("acme.notifications.v1.NotificationValidationService/PreviewNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "audit", "created_at"),
                ProbeSupport.value(sent3, "notification", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/NormalizeRetentionPolicy"), "retention_policy", "owner", "display_name"),
                ProbeSupport.value(sent3, "notification", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "audit", "updated_at"),
                ProbeSupport.value(sent3, "notification", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineReportingService/ListBudgetLineReports"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport"), "mileage_claim", "billing_address"),
                ProbeSupport.value(response, "expense_report", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/NormalizeRetentionPolicy"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/PreviewNotification"), "notification", "audit", "updated_by"),
                ProbeSupport.value(response, "expense_report", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report_id"),
                ProbeSupport.value(response, "expense_report", "expense_report_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "due_at"),
                ProbeSupport.value(response, "expense_report", "due_at"));
    }
}
