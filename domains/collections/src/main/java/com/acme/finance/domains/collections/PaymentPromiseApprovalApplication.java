package com.acme.finance.domains.collections;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ListPendingMandatesRequest;
import com.acme.banking.v1.ListPendingMandatesResponse;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.CountPaymentPromisesRequest;
import com.acme.collections.v1.CountPaymentPromisesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusRequest;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RecallPaymentPromiseRequest;
import com.acme.collections.v1.RecallPaymentPromiseResponse;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitPaymentPromiseRequest;
import com.acme.collections.v1.SubmitPaymentPromiseResponse;
import com.acme.collections.v1.SuggestPaymentPromisesRequest;
import com.acme.collections.v1.SuggestPaymentPromisesResponse;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.collections.v1.WatchCollectionCasesRequest;
import com.acme.collections.v1.WatchCollectionCasesResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Routes payment promises through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PaymentPromiseApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentPromiseApprovalApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking;

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceFutureStub collectionsPaymentPromiseQueryServiceFuture;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    public PaymentPromiseApprovalApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking,
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceFutureStub collectionsPaymentPromiseQueryServiceFuture,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.collectionsPaymentPromiseQueryServiceBlocking = collectionsPaymentPromiseQueryServiceBlocking;
        this.collectionsPaymentPromiseQueryServiceFuture = collectionsPaymentPromiseQueryServiceFuture;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ApprovePaymentPromiseStage0Requested(ListPendingMandatesRequest request, CompletableFuture<ListPendingMandatesResponse> completion, io.grpc.Context context) {}

    public record RejectPaymentPromiseStage0Requested(NormalizeWriteOffRequest request, CompletableFuture<NormalizeWriteOffResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a payment promise.
     */
    public SubmitPaymentPromiseResponse submitPaymentPromise(SubmitPaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = FindPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentPromise().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPaymentPromise().getMetadataMap())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPaymentPromises(value));

        var outbound1 = ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentPromise().getAudit().getCreatedAt())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<ListRetentionPolicyVersionsRequest, ListRetentionPolicyVersionsResponse>() {
            @Override
            public ListRetentionPolicyVersionsResponse apply(ListRetentionPolicyVersionsRequest input) {
                return auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyVersions(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input2 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPaymentPromise().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPaymentPromise().getAudit().getCreatedBy())
                                .setEtag(request.getPaymentPromise().getAudit().getEtag())
                                .setUpdatedAt(request.getPaymentPromise().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPaymentPromise", request.getPaymentPromise().getPaymentPromiseId());
        return SubmitPaymentPromiseResponse.newBuilder()
                .setOutcome(SubmitPaymentPromiseResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .putAllMetadata(request.getPaymentPromise().getMetadataMap())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a payment promise.
     */
    public ApprovePaymentPromiseResponse approvePaymentPromise(ApprovePaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = ListPendingMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingMandatesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentPromise().getAudit().getCreatedAt())
                        .putAllLabels(request.getPaymentPromise().getMetadataMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListPendingMandatesResponse>();
        events.publishEvent(new ApprovePaymentPromiseStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CloneCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getPaymentPromise().getOrganizer().getEmail())
                                .setUserId(request.getPaymentPromise().getOrganizer().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentPromise().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.CollectionCaseService");
        var selected1 = CollectionCaseServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.cloneCollectionCase(outbound1);

        var outbound2 = WatchCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchCollectionCasesRequest.Filter.newBuilder()
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getPaymentPromise().getOrganizer().getEmail())
                        .setName(reply1.getCollectionCase().getRequester().getName())
                        .build())
                .build();
        var stream2 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCollectionCases(outbound2);
        var reply2 = WatchCollectionCasesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = AdvancePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvancePolicyWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getCollectionCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPaymentPromise().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentPromise().getOrganizer().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply1.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<AdvancePolicyWorkflowRequest, AdvancePolicyWorkflowResponse>() {
            @Override
            public AdvancePolicyWorkflowResponse apply(AdvancePolicyWorkflowRequest input) {
                return insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePolicyWorkflow(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePaymentPromise", request.getPaymentPromise().getPaymentPromiseId());
        return ApprovePaymentPromiseResponse.newBuilder()
                .setOutcome(ApprovePaymentPromiseResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .putAllMetadata(request.getPaymentPromise().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getCollectionCase().getRequester().getEmail())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply3.getPolicy().getContact().getPhone())
                                        .build())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a payment promise.
     */
    public RejectPaymentPromiseResponse rejectPaymentPromise(RejectPaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = NormalizeWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentPromise().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getPaymentPromise().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(request.getPaymentPromise().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<NormalizeWriteOffResponse>();
        events.publishEvent(new RejectPaymentPromiseStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPaymentPromise", request.getPaymentPromise().getPaymentPromiseId());
        return RejectPaymentPromiseResponse.newBuilder()
                .setOutcome(RejectPaymentPromiseResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setStatus(request.getPaymentPromise().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a payment promise.
     */
    public EscalatePaymentPromiseResponse escalatePaymentPromise(EscalatePaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = StreamUnderwritingMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamUnderwritingMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingMetrics(outbound0);
        var reply0 = StreamUnderwritingMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SummarizeCollectionCasesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.CollectionCaseQueryService");
        var serviceName1 = "acme.collections.v1" + "." + "CollectionCaseQueryService";
        var methodName1 = "Sum" + "marizeCollectionCases";
        var descriptor1 = MethodDescriptor.<SummarizeCollectionCasesRequest, SummarizeCollectionCasesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SummarizeCollectionCasesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SummarizeCollectionCasesResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePaymentPromise", request.getPaymentPromise().getPaymentPromiseId());
        return EscalatePaymentPromiseResponse.newBuilder()
                .setOutcome(EscalatePaymentPromiseResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setWindow(request.getPaymentPromise().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists payment promises a page at a time.
     */
    public ListPendingPaymentPromisesResponse listPendingPaymentPromises(ListPendingPaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "ListPendingPaymentPromises");

        var outbound0 = FindPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var reply0 = collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPaymentPromises(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPaymentPromises", request.getFilter().getQuery());
        return ListPendingPaymentPromisesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalls a payment promise.
     */
    public RecallPaymentPromiseResponse recallPaymentPromise(RecallPaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        List<SuggestPaymentPromisesResponse> results0 = request.getPaymentPromise().getParticipantsList().stream()
                .map(element -> SuggestPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentPromise().getAudit().getUpdatedAt())
                        .setOwnerId(element.getParticipantId())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestPaymentPromises(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SuggestPaymentPromisesResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPaymentPromise", request.getPaymentPromise().getPaymentPromiseId());
        return RecallPaymentPromiseResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .putAllMetadata(request.getPaymentPromise().getMetadataMap())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .build();
    }

    /**
     * Returns a payment promise.
     */
    public GetPaymentPromiseApprovalStatusResponse getPaymentPromiseApprovalStatus(GetPaymentPromiseApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPaymentPromiseApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getPaymentPromiseId(), "GetPaymentPromiseApprovalStatus");

        var outbound0 = JsonCodec.roundTrip(SubmitCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setPaymentPromiseId(request.getPaymentPromiseId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SubmitCollectionCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), SubmitCollectionCaseRequest.class);
        var reply0 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitCollectionCase(outbound0);
        return GetPaymentPromiseApprovalStatusResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCollectionCase().getAudit().getCreatedAt())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .build();
    }

    /**
     * Comments a payment promise.
     */
    public CommentPaymentPromiseApprovalResponse commentPaymentPromiseApproval(CommentPaymentPromiseApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPaymentPromiseApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = CountPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPaymentPromisesRequest.Filter.newBuilder()
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPaymentPromises(outbound0), rpcExecutor);

        var outbound1 = ScoreWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPaymentPromise().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getPaymentPromise().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreWriteOff(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BrowsePaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePaymentPromisesRequest.Filter.newBuilder()
                        .setRegion(reply1.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = collectionsPaymentPromiseQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browsePaymentPromises(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPaymentPromiseApproval", request.getPaymentPromise().getPaymentPromiseId());
        return CommentPaymentPromiseApprovalResponse.newBuilder()
                .setOutcome(CommentPaymentPromiseApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setStatus(request.getPaymentPromise().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
