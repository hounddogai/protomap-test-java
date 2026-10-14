package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.dispatch.v1.BrowseDispatchOrdersRequest;
import com.acme.dispatch.v1.BrowseDispatchOrdersResponse;
import com.acme.dispatch.v1.DispatchOrderQueryServiceGrpc;
import com.acme.expenses.v1.ActivateReceiptRequest;
import com.acme.expenses.v1.ActivateReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.expenses.v1.ExplainReceiptDecisionRequest;
import com.acme.expenses.v1.ExplainReceiptDecisionResponse;
import com.acme.expenses.v1.GetReceiptLifecycleRequest;
import com.acme.expenses.v1.GetReceiptLifecycleResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReinstateReceiptRequest;
import com.acme.expenses.v1.ReinstateReceiptResponse;
import com.acme.expenses.v1.RenewReceiptRequest;
import com.acme.expenses.v1.RenewReceiptResponse;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.expenses.v1.SuspendReceiptRequest;
import com.acme.expenses.v1.SuspendReceiptResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ReceiptLifecycleApplicationTest {
    @Test
    void renewReceiptPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ReceiptLifecycleApplicationEvents(DispatchOrderQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReceiptLifecycleApplication.ExpireReceiptStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReceiptLifecycleApplication(
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                ControlWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimReportingServiceGrpc.newFutureStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(RenewReceiptRequest.getDefaultInstance());
        var response = application.renewReceipt(request);
        var sent0 = channel.request("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "control", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "control", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "details", "notify", "phone"));
        var sent1 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/StreamRetentionPolicyNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent1, "subscriber", "phone"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetApprovalService/CommentBudgetApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent2, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent2, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow"), "control", "discount", "amount"),
                ProbeSupport.value(sent2, "budget", "limits", "amount"));
        var sent3 = channel.request("acme.expenses.v1.ReceiptValidationService/CheckReceiptEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow"), "control", "audit", "updated_at"),
                ProbeSupport.value(sent3, "receipt", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/CommentBudgetApproval"), "budget", "audit", "created_at"),
                ProbeSupport.value(sent3, "receipt", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow"), "control", "discount", "amount"),
                ProbeSupport.value(sent3, "receipt", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/CancelControlWorkflow"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/CommentBudgetApproval"), "budget", "holder", "display_name"),
                ProbeSupport.value(response, "receipt", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/CommentBudgetApproval"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/CheckReceiptEligibility"), "receipt", "audit", "updated_at"),
                ProbeSupport.value(response, "receipt", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/CheckReceiptEligibility"), "receipt", "status"),
                ProbeSupport.value(response, "receipt", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt_id"),
                ProbeSupport.value(response, "receipt", "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReceiptLifecycleApplicationEvents(DispatchOrderQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BrowseDispatchOrdersResponse>();
        listener.on(new ReceiptLifecycleApplication.ExpireReceiptStage0Requested(ProbeSupport.sample(BrowseDispatchOrdersRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
