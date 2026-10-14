package com.acme.finance.domains.contracts;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CloneAuditEventRequest;
import com.acme.audit.v1.CloneAuditEventResponse;
import com.acme.budgeting.v1.ApproveBudgetRequest;
import com.acme.budgeting.v1.ApproveBudgetResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.AdvanceRenewalWorkflowRequest;
import com.acme.contracts.v1.AdvanceRenewalWorkflowResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.CancelRenewalWorkflowRequest;
import com.acme.contracts.v1.CancelRenewalWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.DelegateRenewalApprovalRequest;
import com.acme.contracts.v1.DelegateRenewalApprovalResponse;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.contracts.v1.EscalateObligationRequest;
import com.acme.contracts.v1.EscalateObligationResponse;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.InspectRenewalWorkflowRequest;
import com.acme.contracts.v1.InspectRenewalWorkflowResponse;
import com.acme.contracts.v1.ListRenewalWorkflowsRequest;
import com.acme.contracts.v1.ListRenewalWorkflowsResponse;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PauseRenewalWorkflowRequest;
import com.acme.contracts.v1.PauseRenewalWorkflowResponse;
import com.acme.contracts.v1.RejectObligationRequest;
import com.acme.contracts.v1.RejectObligationResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalApprovalServiceGrpc;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.contracts.v1.RetryContractClauseWorkflowRequest;
import com.acme.contracts.v1.RetryContractClauseWorkflowResponse;
import com.acme.contracts.v1.RetryRenewalWorkflowRequest;
import com.acme.contracts.v1.RetryRenewalWorkflowResponse;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.StartRenewalWorkflowRequest;
import com.acme.contracts.v1.StartRenewalWorkflowResponse;
import com.acme.contracts.v1.SubmitObligationRequest;
import com.acme.contracts.v1.SubmitObligationResponse;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RenewalWorkflowApplicationTest {
    @Test
    void retryRenewalWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new RenewalWorkflowApplicationEvents(ObligationApprovalServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RenewalWorkflowApplication.RetryRenewalWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RenewalWorkflowApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ContractClauseApprovalServiceGrpc.newBlockingStub(channel),
                ContractClauseWorkflowServiceGrpc.newBlockingStub(channel),
                ContractWorkflowServiceGrpc.newBlockingStub(channel),
                ObligationApprovalServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newFutureStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalApprovalServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                new RenewalWorkflowRequestMapperImpl(),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetryRenewalWorkflowRequest.getDefaultInstance());
        var response = application.retryRenewalWorkflow(request);
        var sent0 = channel.request("acme.contracts.v1.ContractWorkflowService/CancelContractWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "created_at"),
                ProbeSupport.value(sent0, "contract", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "created_by"),
                ProbeSupport.value(sent0, "contract", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        var sent1 = channel.request("acme.contracts.v1.ObligationApprovalService/RejectObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent1, "obligation", "customer", "email"));
        var sent2 = channel.request("acme.contracts.v1.RenewalApprovalService/DelegateRenewalApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "obligation", "assignee", "email"),
                ProbeSupport.value(sent2, "renewal", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractWorkflowService/CancelContractWorkflow"), "contract", "billing_address", "region"),
                ProbeSupport.value(sent2, "renewal", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent2, "renewal", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "obligation", "customer", "email"),
                ProbeSupport.value(sent2, "renewal", "organizer", "email"));
        var sent3 = channel.request("acme.customers.v1.PreferenceValidationService/PreviewPreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "participants", "contact", "email"),
                ProbeSupport.value(sent3, "preference", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "participants", "contact", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "participants", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractWorkflowService/CancelContractWorkflow"), "contract", "billing_address", "postal_code"),
                ProbeSupport.value(response, "renewal", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractWorkflowService/CancelContractWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "obligation", "assignee", "user_id"),
                ProbeSupport.value(response, "renewal", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalApprovalService/DelegateRenewalApproval"), "renewal", "metadata"),
                ProbeSupport.value(response, "renewal", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalApprovalService/DelegateRenewalApproval"), "renewal", "location", "recipient"),
                ProbeSupport.value(response, "renewal", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal_id"),
                ProbeSupport.value(response, "renewal", "renewal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "window"),
                ProbeSupport.value(response, "renewal", "window"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RenewalWorkflowApplicationEvents(ObligationApprovalServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RejectObligationResponse>();
        listener.on(new RenewalWorkflowApplication.RetryRenewalWorkflowStage1Requested(ProbeSupport.sample(RejectObligationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
