package com.acme.finance.domains.expenses;

import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.ValidateConsentRecordRequest;
import com.acme.customers.v1.ValidateConsentRecordResponse;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowRequest;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowResponse;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.CancelMileageClaimWorkflowRequest;
import com.acme.expenses.v1.CancelMileageClaimWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.InspectMileageClaimWorkflowRequest;
import com.acme.expenses.v1.InspectMileageClaimWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.PushExpensePolicyRequest;
import com.acme.expenses.v1.PushExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryExpenseReportWorkflowRequest;
import com.acme.expenses.v1.RetryExpenseReportWorkflowResponse;
import com.acme.expenses.v1.RetryMileageClaimWorkflowRequest;
import com.acme.expenses.v1.RetryMileageClaimWorkflowResponse;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.expenses.v1.StartMileageClaimWorkflowRequest;
import com.acme.expenses.v1.StartMileageClaimWorkflowResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class MileageClaimWorkflowApplicationTest {
    @Test
    void signalMileageClaimWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimWorkflowApplication(
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                ExpenseReportWorkflowServiceGrpc.newBlockingStub(channel),
                ReceiptLifecycleServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newFutureStub(channel),
                PremiumSyncServiceGrpc.newStub(channel),
                InvoiceBatchServiceGrpc.newStub(channel),
                repository);
        var request = ProbeSupport.sample(SignalMileageClaimWorkflowRequest.getDefaultInstance());
        var response = application.signalMileageClaimWorkflow(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportWorkflowService/RetryExpenseReportWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "updated_by"),
                ProbeSupport.value(sent0, "expense_report", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "expense_report", "billing_address", "postal_code"));
        var sent1 = channel.request("acme.expenses.v1.ReceiptLifecycleService/ScheduleReceiptExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/RetryExpenseReportWorkflow"), "expense_report", "billing_address", "lines"),
                ProbeSupport.value(sent1, "receipt", "billing_address", "lines"));
        var sent2 = channel.request("acme.insurance.v1.PolicyApprovalService/CommentPolicyApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "etag"),
                ProbeSupport.value(sent2, "policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportWorkflowService/RetryExpenseReportWorkflow"), "expense_report", "issued_at"),
                ProbeSupport.value(response, "mileage_claim", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptLifecycleService/ScheduleReceiptExpiry"), "receipt", "receipt_id"),
                ProbeSupport.value(response, "mileage_claim", "counterparty", "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/CommentPolicyApproval"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/CommentPolicyApproval"), "policy", "holder", "display_name"),
                ProbeSupport.value(response, "mileage_claim", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim_id"),
                ProbeSupport.value(response, "mileage_claim", "mileage_claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address"),
                ProbeSupport.value(response, "mileage_claim", "billing_address"));
    }
}
