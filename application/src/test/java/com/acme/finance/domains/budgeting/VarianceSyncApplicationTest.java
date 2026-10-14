package com.acme.finance.domains.budgeting;

import com.acme.banking.v1.CancelStandingOrderWorkflowRequest;
import com.acme.banking.v1.CancelStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.GetVarianceSyncStateRequest;
import com.acme.budgeting.v1.GetVarianceSyncStateResponse;
import com.acme.budgeting.v1.ListVarianceConflictsRequest;
import com.acme.budgeting.v1.ListVarianceConflictsResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.PullVariancesRequest;
import com.acme.budgeting.v1.PullVariancesResponse;
import com.acme.budgeting.v1.PushVarianceRequest;
import com.acme.budgeting.v1.PushVarianceResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.ReconcileVariancesRequest;
import com.acme.budgeting.v1.ReconcileVariancesResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.WatchBudgetsRequest;
import com.acme.budgeting.v1.WatchBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateClaimRequest;
import com.acme.insurance.v1.EscalateClaimResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.RenewPolicyRequest;
import com.acme.insurance.v1.RenewPolicyResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.risk.v1.CancelRiskRegisterWorkflowRequest;
import com.acme.risk.v1.CancelRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskStatusChange;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
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

class VarianceSyncApplicationTest {
    @Test
    void pushVariancePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new VarianceSyncApplication(
                StandingOrderWorkflowServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PushVarianceRequest.getDefaultInstance());
        var response = application.pushVariance(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "audit", "updated_at"),
                ProbeSupport.value(sent0, "policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester", "preferred_channel"),
                ProbeSupport.value(sent0, "policy", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierService/CreateSupplier");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "assignee", "user_id"),
                ProbeSupport.value(sent1, "supplier", "relationship_manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow"), "policy", "contact", "name"),
                ProbeSupport.value(sent1, "supplier", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow"), "policy", "holder", "display_name"),
                ProbeSupport.value(sent1, "supplier", "relationship_manager", "display_name"));
        var sent2 = channel.request("acme.insurance.v1.PolicyLifecycleService/RenewPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "sla", "policy_id"),
                ProbeSupport.value(sent2, "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/CreateSupplier"), "supplier", "relationship_manager", "user_id"),
                ProbeSupport.value(sent2, "details", "new_owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/StartPolicyWorkflow"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/CreateSupplier"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyLifecycleService/RenewPolicy"), "policy", "audit", "created_at"),
                ProbeSupport.value(response, "variance", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyLifecycleService/RenewPolicy"), "policy", "contact", "preferred_channel"),
                ProbeSupport.value(response, "variance", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance_id"),
                ProbeSupport.value(response, "variance", "variance_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "custom_fields"),
                ProbeSupport.value(response, "variance", "custom_fields"));
    }

    @Test
    void streamVarianceChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new VarianceSyncApplication(
                StandingOrderWorkflowServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamVarianceChangesRequest.getDefaultInstance());
        var response = application.streamVarianceChanges(request);
        var sent0 = channel.request("acme.lending.v1.CollateralWorkflowService/ListCollateralWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "history", "changed_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "customer", "budget_line_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetService/WatchBudgets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "requester", "phone"),
                ProbeSupport.value(sent1, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "custom_fields"),
                ProbeSupport.value(response, "variance", "custom_fields"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        var endpoint = new VarianceSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamVarianceChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamVarianceChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamVarianceChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
