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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a renewal from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RenewalWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(RenewalWorkflowApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking;

    private final ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking;

    private final ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking;

    private final ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceFutureStub contractsObligationCalculationServiceFuture;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalApprovalServiceGrpc.RenewalApprovalServiceBlockingStub contractsRenewalApprovalServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final RenewalWorkflowRequestMapper mapper;

    private final ContractsRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public RenewalWorkflowApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking,
            ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking,
            ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking,
            ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceFutureStub contractsObligationCalculationServiceFuture,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalApprovalServiceGrpc.RenewalApprovalServiceBlockingStub contractsRenewalApprovalServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            RenewalWorkflowRequestMapper mapper,
            ContractsRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.contractsContractClauseApprovalServiceBlocking = contractsContractClauseApprovalServiceBlocking;
        this.contractsContractClauseWorkflowServiceBlocking = contractsContractClauseWorkflowServiceBlocking;
        this.contractsContractWorkflowServiceBlocking = contractsContractWorkflowServiceBlocking;
        this.contractsObligationApprovalServiceBlocking = contractsObligationApprovalServiceBlocking;
        this.contractsObligationCalculationServiceFuture = contractsObligationCalculationServiceFuture;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalApprovalServiceBlocking = contractsRenewalApprovalServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    public record RetryRenewalWorkflowStage1Requested(RejectObligationRequest request, CompletableFuture<RejectObligationResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a renewal.
     */
    public StartRenewalWorkflowResponse startRenewalWorkflow(StartRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = ScoreRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getRenewal().getOrganizer().getEmail())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRenewal(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = EscalateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateObligationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getRenewal().getOrganizer().getUserId())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getRenewal().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsObligationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateObligation(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input2 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getRenewal().getOrganizer())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                                .setCreatedBy(request.getRenewal().getAudit().getCreatedBy())
                                .setEtag(request.getRenewal().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartRenewalWorkflow", request.getRenewal().getRenewalId());
        return StartRenewalWorkflowResponse.newBuilder()
                .setOutcome(StartRenewalWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(request.getRenewal().getLocation())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a renewal.
     */
    public AdvanceRenewalWorkflowResponse advanceRenewalWorkflow(AdvanceRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = CountMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRenewal().getAudit().getUpdatedAt())
                        .putAllLabels(request.getRenewal().getMetadataMap())
                        .setOwnerId(request.getRenewalId())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.metrics.v1.MetricSeriesQueryService");
        var serviceName0 = "acme.metrics.v1" + "." + "MetricSeriesQueryService";
        var methodName0 = "Cou" + "ntMetricSerieses";
        var descriptor0 = MethodDescriptor.<CountMetricSeriesesRequest, CountMetricSeriesesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CountMetricSeriesesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountMetricSeriesesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        List<SubmitObligationResponse> results1 = request.getRenewal().getParticipantsList().stream()
                .map(element -> SubmitObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitObligationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contractsObligationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitObligation(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? SubmitObligationResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = EscalateContractClauseRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getObligation().getAssignee().getDisplayName())
                                .setEmail(reply1.getObligation().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<EscalateContractClauseRequest, EscalateContractClauseResponse>() {
            @Override
            public EscalateContractClauseResponse apply(EscalateContractClauseRequest input) {
                return contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateContractClause(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = mapper.advanceRenewalWorkflowStage3(request, reply0, reply1);
        var reply3 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneAuditEvent(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceRenewalWorkflow", request.getRenewal().getRenewalId());
        return AdvanceRenewalWorkflowResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getContractClause().getAudit().getCreatedAt())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a renewal.
     */
    public CancelRenewalWorkflowResponse cancelRenewalWorkflow(CancelRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        List<RetryContractClauseWorkflowResponse> results0 = request.getRenewal().getParticipantsList().stream()
                .map(element -> RetryContractClauseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryContractClauseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contractsContractClauseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryContractClauseWorkflow(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RetryContractClauseWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getRenewal().getParticipantsList().stream()
                .map(element -> NormalizeObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeObligationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeObligation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? NormalizeObligationResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelRenewalWorkflow", request.getRenewal().getRenewalId());
        return CancelRenewalWorkflowResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .putAllMetadata(request.getRenewal().getMetadataMap())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .build();
    }

    /**
     * Retrys a renewal.
     */
    public RetryRenewalWorkflowResponse retryRenewalWorkflow(RetryRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var builder0 = CancelContractWorkflowRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setContract(Contract.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                        .setCreatedBy(request.getRenewal().getAudit().getCreatedBy())
                        .build())
                .build());
        builder0.setDetails(CancelContractWorkflowRequest.Details.newBuilder()
                .setNotify(ContactInfo.newBuilder()
                        .setName(request.getDetails().getNotify().getName())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = contractsContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelContractWorkflow(outbound0);

        var outbound1 = RejectObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<RejectObligationResponse>();
        events.publishEvent(new RetryRenewalWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = RpcSupport.unpack(Any.pack(DelegateRenewalApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getContract().getBillingAddress().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getObligation().getCustomer().getEmail())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getObligation().getAssignee().getEmail())
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .build())
                                .build())
                        .build())
                .build()), DelegateRenewalApprovalRequest.class);
        var reply2 = contractsRenewalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateRenewalApproval(outbound2);

        var results3 = new ArrayList<PreviewPreferenceResponse>();
        for (var element : request.getRenewal().getParticipantsList()) {
            PreviewPreferenceRequest itemRequest = PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build();
            results3.add(customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(itemRequest));
        }
        var reply3 = results3.isEmpty() ? PreviewPreferenceResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryRenewalWorkflow", request.getRenewal().getRenewalId());
        return RetryRenewalWorkflowResponse.newBuilder()
                .setOutcome(RetryRenewalWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getContract().getBillingAddress().getPostalCode())
                                .setRecipient(reply2.getRenewal().getLocation().getRecipient())
                                .build())
                        .putAllMetadata(reply2.getRenewal().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply1.getObligation().getAssignee().getUserId())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Pauses a renewal.
     */
    public PauseRenewalWorkflowResponse pauseRenewalWorkflow(PauseRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        List<FindCustomersResponse> results0 = request.getRenewal().getParticipantsList().stream()
                .map(element -> FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .setOwnerId(element.getParticipantId())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findCustomers(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? FindCustomersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(ApproveBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ApproveBudgetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getRenewal().getOrganizer().getDisplayName())
                                .setEmail(request.getRenewal().getOrganizer().getEmail())
                                .build())
                        .build())
                .build(), ApproveBudgetRequest.class);
        var reply1 = budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveBudget(outbound1);

        var outbound2 = JsonCodec.roundTrip(BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .putAllLabels(reply1.getBudget().getLabelsMap())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build(), BrowseSearchIndexesRequest.class);
        var reply2 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseRenewalWorkflow", request.getRenewal().getRenewalId());
        return PauseRenewalWorkflowResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setStatus(request.getRenewal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a renewal.
     */
    public ResumeRenewalWorkflowResponse resumeRenewalWorkflow(ResumeRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateCustomerSegmentsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getRenewal().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RecalculateCustomerSegmentsResponse fetch(RecalculateCustomerSegmentsRequest input) {
                return customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeRenewalWorkflow", request.getRenewal().getRenewalId());
        return ResumeRenewalWorkflowResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .setStatus(request.getRenewal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a renewal.
     */
    public InspectRenewalWorkflowResponse inspectRenewalWorkflow(InspectRenewalWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectRenewalWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Renewal", request.getRenewalId(), "InspectRenewalWorkflow");

        var outbound0 = ScoreObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setSubject(request.getRenewalId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.contracts.v1.ObligationValidationService/ScoreObligation", outbound0);
        var reply0 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreObligation(command0.payload());

        var outbound1 = ExportSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getObligation().getAudit().getUpdatedAt())
                        .build())
                .build();
        var stream1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSearchIndexes(outbound1);
        var reply1 = ExportSearchIndexesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        return InspectRenewalWorkflowResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists renewals a page at a time.
     */
    public ListRenewalWorkflowsResponse listRenewalWorkflows(ListRenewalWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRenewalWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Renewal", request.getContext().getTraceId(), "ListRenewalWorkflows");

        var outbound0 = EstimateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateObligationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addComments(ObligationComment.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var pending0 = contractsObligationCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).estimateObligation(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = RpcSupport.unpack(Any.pack(CheckCustomerSegmentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getObligation().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build()), CheckCustomerSegmentEligibilityRequest.class);
        var reply1 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerSegmentEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRenewalWorkflows", request.getPage().getPageToken());
        return ListRenewalWorkflowsResponse.newBuilder()
                .addRenewals(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getCustomerSegment().getOwner().getEmail())
                                        .setName(reply0.getObligation().getRequester().getName())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
