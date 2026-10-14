package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.ActivateReconciliationRequest;
import com.acme.accounting.v1.ActivateReconciliationResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.ExpireReconciliationRequest;
import com.acme.accounting.v1.ExpireReconciliationResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.GetReconciliationLifecycleRequest;
import com.acme.accounting.v1.GetReconciliationLifecycleResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.RenewReconciliationRequest;
import com.acme.accounting.v1.RenewReconciliationResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.SuspendReconciliationRequest;
import com.acme.accounting.v1.SuspendReconciliationResponse;
import com.acme.accounting.v1.TerminateReconciliationRequest;
import com.acme.accounting.v1.TerminateReconciliationResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payments.v1.PaymentQueryServiceGrpc;
import com.acme.payments.v1.SuggestPaymentsRequest;
import com.acme.payments.v1.SuggestPaymentsResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ReconciliationLifecycleApplicationTest {
    @Test
    void expireReconciliationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new ReconciliationLifecycleApplicationEvents(MileageClaimLifecycleServiceGrpc.newStub(channel), ExposureWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReconciliationLifecycleApplication.RenewReconciliationStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ReconciliationLifecycleApplication.ExpireReconciliationStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReconciliationLifecycleApplication(
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsQueryServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingSyncServiceGrpc.newStub(channel),
                PaymentQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newStub(channel),
                repository);
        var request = ProbeSupport.sample(ExpireReconciliationRequest.getDefaultInstance());
        var response = application.expireReconciliation(request);
        var sent0 = channel.request("acme.accounting.v1.ChartOfAccountsQueryService/FindChartOfAccountses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent1, "exposure", "contact", "name"));
        var sent2 = channel.request("acme.accounting.v1.ReconciliationValidationService/PreviewReconciliation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent2, "reconciliation", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "audit", "etag"),
                ProbeSupport.value(sent2, "reconciliation", "audit", "etag"));
        var sent3 = channel.request("acme.accounting.v1.AllocationValidationService/ExplainAllocationDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "audit", "created_by"),
                ProbeSupport.value(sent3, "allocation", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/PreviewReconciliation"), "reconciliation", "requester", "email"),
                ProbeSupport.value(sent3, "allocation", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/PreviewReconciliation"), "reconciliation", "audit", "created_at"),
                ProbeSupport.value(sent3, "allocation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsQueryService/FindChartOfAccountses"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "holder", "email"),
                ProbeSupport.value(response, "reconciliation", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/PreviewReconciliation"), "reconciliation", "audit", "created_at"),
                ProbeSupport.value(response, "reconciliation", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/ExplainAllocationDecision"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation_id"),
                ProbeSupport.value(response, "reconciliation", "reconciliation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(response, "reconciliation", "assignee", "display_name"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReconciliationLifecycleApplicationEvents(MileageClaimLifecycleServiceGrpc.newStub(channel), ExposureWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ScheduleMileageClaimExpiryResponse>();
        listener.on(new ReconciliationLifecycleApplication.RenewReconciliationStage0Requested(ProbeSupport.sample(ScheduleMileageClaimExpiryRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<AdvanceExposureWorkflowResponse>();
        listener.on(new ReconciliationLifecycleApplication.ExpireReconciliationStage1Requested(ProbeSupport.sample(AdvanceExposureWorkflowRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
