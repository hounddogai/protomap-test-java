package com.acme.finance.domains.contracts;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.budgeting.v1.BudgetLine;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes obligations through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ObligationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ObligationApprovalApplication.class);

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking;

    private final ApplicationEventPublisher events;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final ContractsRepository repository;

    private final SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking;

    public ObligationApprovalApplication(
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            ChannelRegistry channels,
            ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking,
            ApplicationEventPublisher events,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            ContractsRepository repository,
            SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking) {
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.channels = channels;
        this.contractsContractClauseWorkflowServiceBlocking = contractsContractClauseWorkflowServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.dispatchDispatchOrderServiceBlocking = dispatchDispatchOrderServiceBlocking;
        this.events = events;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.repository = repository;
        this.surveysSurveyQueryServiceBlocking = surveysSurveyQueryServiceBlocking;
    }

    public record EscalateObligationStage3Requested(RetryBudgetLineWorkflowRequest request, CompletableFuture<RetryBudgetLineWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an obligation.
     */
    public SubmitObligationResponse submitObligation(SubmitObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = ListContractClauseWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListContractClauseWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getObligation().getAudit().getUpdatedAt())
                        .setOwnerId(request.getObligation().getAssignee().getUserId())
                        .build())
                .build();
        final class Stage0Adapter {
            ListContractClauseWorkflowsResponse fetch(ListContractClauseWorkflowsRequest input) {
                return contractsContractClauseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listContractClauseWorkflows(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getObligation().getAssignee().getDisplayName())
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .setUserId(request.getObligation().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        Function<ValidateCustomerSegmentRequest, ValidateCustomerSegmentResponse> operation1 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateCustomerSegment;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitObligation", request.getObligation().getObligationId());
        return SubmitObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCustomerSegment().getAudit().getUpdatedBy())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .setOutcome(SubmitObligationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves an obligation.
     */
    public ApproveObligationResponse approveObligation(ApproveObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = CloneDispatchOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDispatchOrder(DispatchOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getObligation().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(ZoneRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<CloneDispatchOrderRequest, CloneDispatchOrderResponse> port0 = RpcPort.adapt(dispatchDispatchOrderServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneDispatchOrder);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveObligation", request.getObligation().getObligationId());
        return ApproveObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setStatus(request.getObligation().getStatus())
                        .build())
                .setOutcome(ApproveObligationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Rejects an obligation.
     */
    public RejectObligationResponse rejectObligation(RejectObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<CreateCustomerRequest, CreateCustomerResponse> port0 = RpcPort.adapt(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createCustomer);
        var reply0 = port0.exchange(outbound0);

        var builder1 = CancelContractClauseWorkflowRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setContractClause(ContractClause.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(reply0.getCustomer().getAudit().getUpdatedBy())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = contractsContractClauseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelContractClauseWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectObligation", request.getObligation().getObligationId());
        return RejectObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .addComments(ObligationComment.newBuilder()
                                .setCreatedAt(reply1.getContractClause().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Escalates an obligation.
     */
    public EscalateObligationResponse escalateObligation(EscalateObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = ListDepreciationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getObligation().getAudit().getUpdatedAt())
                        .setOwnerId(request.getObligation().getAssignee().getUserId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationReports(value)).orElseThrow();

        var outbound1 = ResolveJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.retryRead(() -> ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveJournalEntries(outbound1));

        List<CheckCustomerEligibilityResponse> results2 = request.getObligation().getHistoryList().stream()
                .map(element -> CheckCustomerEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .build())
                        .setPassport(CustomerPassport.newBuilder()
                                .setExpiresAt(element.getChangedAt())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckCustomerEligibilityResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = RetryBudgetLineWorkflowRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getCustomer().getPrimaryAddress().getPostalCode())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply2.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion3 = new CompletableFuture<RetryBudgetLineWorkflowResponse>();
        events.publishEvent(new EscalateObligationStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateObligation", request.getObligation().getObligationId());
        return EscalateObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply3.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setStatus(request.getObligation().getStatus())
                        .build())
                .setOutcome(EscalateObligationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls an obligation.
     */
    public RecallObligationResponse recallObligation(RecallObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = (BrowseSurveysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSurveysRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getObligation().getAudit().getCreatedAt())
                        .putAllLabels(request.getObligation().getCustomFieldsMap())
                        .setOwnerId(request.getObligation().getAssignee().getUserId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = surveysSurveyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSurveys(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ResolveSurveysRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveSurveysRequest.class);
        var reply1 = surveysSurveyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSurveys(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallObligation", request.getObligation().getObligationId());
        return RecallObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an obligation.
     */
    public GetObligationApprovalStatusResponse getObligationApprovalStatus(GetObligationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetObligationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Obligation", request.getObligationId(), "GetObligationApprovalStatus");
        return GetObligationApprovalStatusResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .build())
                .build();
    }

    /**
     * Comments an obligation.
     */
    public CommentObligationApprovalResponse commentObligationApproval(CommentObligationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentObligationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = GetObligationRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setObligationId(request.getObligation().getObligationId())
                .build();
        var channel0 = channels.lookup("acme.contracts.v1.ObligationCalculationService");
        var serviceName0 = "acme.contracts.v1" + "." + "ObligationCalculationService";
        var methodName0 = "Get" + "ObligationRates";
        var descriptor0 = MethodDescriptor.<GetObligationRatesRequest, GetObligationRatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(GetObligationRatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetObligationRatesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentObligationApproval", request.getObligation().getSubject());
        return CommentObligationApprovalResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getCustomer().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .build();
    }
}
