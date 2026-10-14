package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.expenses.v1.ActivateExpensePolicyRequest;
import com.acme.expenses.v1.ActivateExpensePolicyResponse;
import com.acme.expenses.v1.BrowseReceiptsRequest;
import com.acme.expenses.v1.BrowseReceiptsResponse;
import com.acme.expenses.v1.DeleteExpensePolicyReportRequest;
import com.acme.expenses.v1.DeleteExpensePolicyReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpireExpensePolicyRequest;
import com.acme.expenses.v1.ExpireExpensePolicyResponse;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.GetExpensePolicyLifecycleRequest;
import com.acme.expenses.v1.GetExpensePolicyLifecycleResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.ReinstateExpensePolicyRequest;
import com.acme.expenses.v1.ReinstateExpensePolicyResponse;
import com.acme.expenses.v1.RenewExpensePolicyRequest;
import com.acme.expenses.v1.RenewExpensePolicyResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.SuspendExpensePolicyRequest;
import com.acme.expenses.v1.SuspendExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class ExpensePolicyLifecycleApplicationTest {
    @Test
    void reinstateExpensePolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpensePolicyLifecycleApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyReportingServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptQueryServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(ReinstateExpensePolicyRequest.getDefaultInstance());
        var response = application.reinstateExpensePolicy(request);
        var sent0 = channel.request("acme.lending.v1.LoanApprovalService/SubmitLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "loan", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "loan", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.customers.v1.HouseholdValidationService/ScoreHousehold");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent1, "household", "children", "children", "manager", "display_name"));
        var sent2 = channel.request("acme.expenses.v1.ReceiptValidationService/NormalizeReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent2, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ScoreHousehold"), "household", "audit", "created_by"),
                ProbeSupport.value(sent2, "receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ScoreHousehold"), "household", "location", "recipient"),
                ProbeSupport.value(sent2, "receipt", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/SubmitLoan"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ScoreHousehold"), "household", "audit", "updated_by"),
                ProbeSupport.value(response, "expense_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ScoreHousehold"), "household", "audit", "etag"),
                ProbeSupport.value(response, "expense_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy_id"),
                ProbeSupport.value(response, "expense_policy", "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
