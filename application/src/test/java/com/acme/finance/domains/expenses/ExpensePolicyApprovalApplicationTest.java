package com.acme.finance.domains.expenses;

import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ScheduleBudgetLineReportRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineReportResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.RecallControlRequest;
import com.acme.compliance.v1.RecallControlResponse;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.expenses.v1.ApproveExpensePolicyRequest;
import com.acme.expenses.v1.ApproveExpensePolicyResponse;
import com.acme.expenses.v1.CommentExpensePolicyApprovalRequest;
import com.acme.expenses.v1.CommentExpensePolicyApprovalResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.CommentMileageClaimApprovalRequest;
import com.acme.expenses.v1.CommentMileageClaimApprovalResponse;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalRequest;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalResponse;
import com.acme.expenses.v1.DelegateExpenseReportApprovalRequest;
import com.acme.expenses.v1.DelegateExpenseReportApprovalResponse;
import com.acme.expenses.v1.EscalateExpensePolicyRequest;
import com.acme.expenses.v1.EscalateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.GetExpensePolicyLifecycleRequest;
import com.acme.expenses.v1.GetExpensePolicyLifecycleResponse;
import com.acme.expenses.v1.ListPendingExpensePoliciesRequest;
import com.acme.expenses.v1.ListPendingExpensePoliciesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.RecallExpensePolicyRequest;
import com.acme.expenses.v1.RecallExpensePolicyResponse;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.ReinstateExpensePolicyRequest;
import com.acme.expenses.v1.ReinstateExpensePolicyResponse;
import com.acme.expenses.v1.RejectExpensePolicyRequest;
import com.acme.expenses.v1.RejectExpensePolicyResponse;
import com.acme.expenses.v1.StreamExpensePolicyMetricsRequest;
import com.acme.expenses.v1.StreamExpensePolicyMetricsResponse;
import com.acme.expenses.v1.SubmitExpensePolicyRequest;
import com.acme.expenses.v1.SubmitExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.modelregistry.v1.CalculateRegisteredModelRequest;
import com.acme.modelregistry.v1.CalculateRegisteredModelResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.SignalRiskScenarioWorkflowRequest;
import com.acme.risk.v1.SignalRiskScenarioWorkflowResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
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

class ExpensePolicyApprovalApplicationTest {
    @Test
    void delegateExpensePolicyApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpensePolicyApprovalApplication(
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                VarianceLifecycleServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                ExpensePolicyLifecycleServiceGrpc.newFutureStub(channel),
                ExpensePolicyReportingServiceGrpc.newBlockingStub(channel),
                ExpenseReportApprovalServiceGrpc.newBlockingStub(channel),
                MileageClaimApprovalServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                RegisteredModelCalculationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(DelegateExpensePolicyApprovalRequest.getDefaultInstance());
        var response = application.delegateExpensePolicyApproval(request);
        var sent0 = channel.request("acme.expenses.v1.ExpensePolicyLifecycleService/ReinstateExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "expense_policy_id"),
                ProbeSupport.value(sent0, "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "email"),
                ProbeSupport.value(sent0, "details", "new_owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "user_id"),
                ProbeSupport.value(sent0, "details", "new_owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "display_name"),
                ProbeSupport.value(sent0, "details", "new_owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(sent0, "details", "effective_at"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyLifecycleService/ReinstateExpensePolicy"), "expense_policy", "owner", "display_name"),
                ProbeSupport.value(sent1, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyLifecycleService/ReinstateExpensePolicy"), "expense_policy", "discount", "amount"),
                ProbeSupport.value(sent1, "expense_report", "lines", "tax", "amount"));
        var sent2 = channel.request("acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval"), "expense_report", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval"), "expense_report", "counterparty", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        var sent3 = channel.request("acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval"), "expense_report", "audit", "updated_at"),
                ProbeSupport.value(sent3, "registered_model", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyLifecycleService/ReinstateExpensePolicy"), "expense_policy", "owner"),
                ProbeSupport.value(response, "expense_policy", "owner"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy_id"),
                ProbeSupport.value(response, "expense_policy", "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "priority"),
                ProbeSupport.value(response, "expense_policy", "priority"));
    }
}
