package com.acme.finance.domains.contracts;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.LintDepreciationsRequest;
import com.acme.assets.v1.LintDepreciationsResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.pipelines.v1.CalculatePipelineRequest;
import com.acme.pipelines.v1.CalculatePipelineResponse;
import com.acme.pipelines.v1.Pipeline;
import com.acme.pipelines.v1.PipelineCalculationServiceGrpc;
import com.acme.pipelines.v1.PipelineParticipant;
import com.acme.surveys.v1.BatchGetSurveysRequest;
import com.acme.surveys.v1.BatchGetSurveysResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an obligation, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ObligationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ObligationCalculationApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final PipelineCalculationServiceGrpc.PipelineCalculationServiceBlockingStub pipelinesPipelineCalculationServiceBlocking;

    private final ContractsRepository repository;

    private final Executor rpcExecutor;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public ObligationCalculationApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            PipelineCalculationServiceGrpc.PipelineCalculationServiceBlockingStub pipelinesPipelineCalculationServiceBlocking,
            ContractsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.pipelinesPipelineCalculationServiceBlocking = pipelinesPipelineCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record SimulateObligationStage0Requested(RecalculateClaimsRequest request, CompletableFuture<RecalculateClaimsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an obligation.
     */
    public CalculateObligationResponse calculateObligation(CalculateObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var builder0 = ValidateUnderwritingRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ValidateUnderwritingRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setName(request.getObligation().getRequester().getName())
                        .build())
                .build());
        builder0.setUnderwriting(Underwriting.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getObligation().getAudit().getUpdatedBy())
                        .build())
                .addComments(UnderwritingComment.newBuilder()
                        .setCreatedAt(request.getObligation().getAudit().getCreatedAt())
                        .build())
                .setSla(UnderwritingSla.newBuilder()
                        .setDueAt(request.getObligation().getSla().getDueAt())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwriting(outbound0);

        var outbound1 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getObligation().getRequester())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input1 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getObligation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getObligation().getAudit().getCreatedBy())
                                .setEtag(request.getObligation().getAudit().getEtag())
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateObligation", request.getObligation().getObligationId());
        return CalculateObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getUnderwriting().getRequester().getEmail())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setStatus(request.getObligation().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates an obligation.
     */
    public EstimateObligationResponse estimateObligation(EstimateObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = BatchGetSurveysRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.surveys.v1.SurveyService");
        var serviceName0 = "acme.surveys.v1" + "." + "SurveyService";
        var methodName0 = "Bat" + "chGetSurveys";
        var descriptor0 = MethodDescriptor.<BatchGetSurveysRequest, BatchGetSurveysResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(BatchGetSurveysRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BatchGetSurveysResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ReconcileRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getObligation().getAssignee().getDisplayName())
                                .setEmail(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getEmail();
                                    default -> "";
                                })
                                .setUserId(request.getObligation().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileRetentionPolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateObligation", request.getObligation().getObligationId());
        return EstimateObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getRetentionPolicy().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes an obligation.
     */
    public QuoteObligationResponse quoteObligation(QuoteObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = ListRetentionPolicySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getObligation().getAudit().getUpdatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicySubscriptions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteObligation", request.getObligation().getObligationId());
        return QuoteObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Compares an obligation.
     */
    public CompareObligationOptionsResponse compareObligationOptions(CompareObligationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareObligationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getObligation().getRequester().getEmail())
                        .setName(request.getObligation().getRequester().getName())
                        .build())
                .build();
        var stream0 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound0);
        var reply0 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        List<CalculatePipelineResponse> results1 = request.getObligation().getHistoryList().stream()
                .map(element -> CalculatePipelineRequest.newBuilder()
                .setContext(request.getContext())
                .setPipeline(Pipeline.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(element.getChangedAt())
                                .build())
                        .addParticipants(PipelineParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getObligation().getRequester().getEmail())
                                        .setName(request.getObligation().getRequester().getName())
                                        .setPreferredChannel(request.getObligation().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(pipelinesPipelineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePipeline(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? CalculatePipelineResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareObligationOptions", request.getObligation().getObligationId());
        return CompareObligationOptionsResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .build())
                .build();
    }

    /**
     * Recalculates an obligation.
     */
    public RecalculateObligationsResponse recalculateObligations(RecalculateObligationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateObligations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var results0 = new ArrayList<ValidateRetentionPolicyResponse>();
        for (var element : request.getObligation().getHistoryList()) {
            ValidateRetentionPolicyRequest itemRequest = ValidateRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getChangedAt())
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
            results0.add(auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRetentionPolicy(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ValidateRetentionPolicyResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SummarizeVenuesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeVenuesRequest.Details.newBuilder()
                        .putAllFilters(request.getObligation().getCustomFieldsMap())
                        .setPeriod(reply0.getRetentionPolicy().getEffective())
                        .build())
                .setReason(reply0.getRetentionPolicy().getBlock().getReason())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateObligations", request.getObligation().getObligationId());
        return RecalculateObligationsResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Simulates an obligation.
     */
    public SimulateObligationResponse simulateObligation(SimulateObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getObligation().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<RecalculateClaimsResponse>();
        events.publishEvent(new SimulateObligationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        List<PreviewCoverageResponse> results1 = request.getObligation().getCommentsList().stream()
                .map(element -> PreviewCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setDetails(PreviewCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCoverage(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? PreviewCoverageResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateObligation", request.getObligation().getObligationId());
        return SimulateObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getCustomer().getDisplayName())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setSubject(request.getObligation().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains an obligation.
     */
    public ExplainObligationCalculationResponse explainObligationCalculation(ExplainObligationCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainObligationCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = RecalculateAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getObligation().getAudit().getEtag())
                                .setUpdatedBy(request.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAssetTransfers(value));

        var outbound1 = JsonCodec.roundTrip(CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getObligation().getAudit().getCreatedAt())
                                .setEtag(reply0.getAssetTransfer().getAudit().getEtag())
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getObligation().getAssignee().getUserId())
                                .build())
                        .build())
                .build(), CalculateCoverageRequest.class);
        var reply1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(outbound1);

        var futures2 = request.getObligation().getCommentsList().stream()
                .map(element -> NormalizeRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRetentionPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRetentionPolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? NormalizeRetentionPolicyResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainObligationCalculation", request.getObligation().getObligationId());
        return ExplainObligationCalculationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .setPriority(reply1.getCoverage().getPriority())
                        .build())
                .setOutcome(ExplainObligationCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an obligation.
     */
    public GetObligationRatesResponse getObligationRates(GetObligationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetObligationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Obligation", request.getObligationId(), "GetObligationRates");

        var outbound0 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeRetentionPolicyUpdates(value));

        var outbound1 = (LintDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply0.getRetentionPolicy().getOwner().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintDepreciations(outbound1);
        return GetObligationRatesResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDepreciation().getAudit().getCreatedAt())
                                .build())
                        .setObligationId(request.getObligationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
