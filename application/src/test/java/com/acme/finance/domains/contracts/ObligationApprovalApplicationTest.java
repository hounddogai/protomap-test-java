package com.acme.finance.domains.contracts;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ApproveObligationRequest;
import com.acme.contracts.v1.ApproveObligationResponse;
import com.acme.contracts.v1.CancelContractClauseWorkflowRequest;
import com.acme.contracts.v1.CancelContractClauseWorkflowResponse;
import com.acme.contracts.v1.CommentObligationApprovalRequest;
import com.acme.contracts.v1.CommentObligationApprovalResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.EscalateObligationRequest;
import com.acme.contracts.v1.EscalateObligationResponse;
import com.acme.contracts.v1.GetObligationApprovalStatusRequest;
import com.acme.contracts.v1.GetObligationApprovalStatusResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.ListContractClauseWorkflowsRequest;
import com.acme.contracts.v1.ListContractClauseWorkflowsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.RecallObligationRequest;
import com.acme.contracts.v1.RecallObligationResponse;
import com.acme.contracts.v1.RejectObligationRequest;
import com.acme.contracts.v1.RejectObligationResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.SubmitObligationRequest;
import com.acme.contracts.v1.SubmitObligationResponse;
import com.acme.customers.v1.CheckCustomerEligibilityRequest;
import com.acme.customers.v1.CheckCustomerEligibilityResponse;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerPassport;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.dispatch.v1.CloneDispatchOrderRequest;
import com.acme.dispatch.v1.CloneDispatchOrderResponse;
import com.acme.dispatch.v1.DispatchOrder;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.dispatch.v1.ZoneRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.surveys.v1.BrowseSurveysRequest;
import com.acme.surveys.v1.BrowseSurveysResponse;
import com.acme.surveys.v1.ResolveSurveysRequest;
import com.acme.surveys.v1.ResolveSurveysResponse;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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

class ObligationApprovalApplicationTest {
    @Test
    void escalateObligationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ObligationApprovalApplicationEvents(BudgetLineWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ObligationApprovalApplication.EscalateObligationStage3Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ObligationApprovalApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ContractClauseWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                DispatchOrderServiceGrpc.newBlockingStub(channel),
                events,
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                repository,
                SurveyQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EscalateObligationRequest.getDefaultInstance());
        var response = application.escalateObligation(request);
        var sent0 = channel.request("acme.assets.v1.DepreciationReportingService/ListDepreciationReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "assignee", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.ledger.v1.JournalEntryQueryService/ResolveJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "requester", "email"),
                ProbeSupport.value(sent2, "customer", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent2, "customer", "relationship_manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "history", "changed_at"),
                ProbeSupport.value(sent2, "customer", "passport", "expires_at"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetLineWorkflowService/RetryBudgetLineWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility"), "customer", "relationship_manager", "display_name"),
                ProbeSupport.value(sent3, "budget_line", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility"), "customer", "primary_address", "postal_code"),
                ProbeSupport.value(sent3, "budget_line", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationReportingService/ListDepreciationReports"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationReportingService/ListDepreciationReports"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/ResolveJournalEntries"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineWorkflowService/RetryBudgetLineWorkflow"), "budget_line", "counterparty", "display_name"),
                ProbeSupport.value(response, "obligation", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation_id"),
                ProbeSupport.value(response, "obligation", "obligation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "status"),
                ProbeSupport.value(response, "obligation", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ObligationApprovalApplicationEvents(BudgetLineWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RetryBudgetLineWorkflowResponse>();
        listener.on(new ObligationApprovalApplication.EscalateObligationStage3Requested(ProbeSupport.sample(RetryBudgetLineWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
