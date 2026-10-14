package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDepreciationRequest;
import com.acme.assets.v1.ApproveDepreciationResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLifecycleServiceGrpc;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DelegateDepreciationApprovalRequest;
import com.acme.assets.v1.DelegateDepreciationApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationLifecycleServiceGrpc;
import com.acme.assets.v1.EscalateDepreciationRequest;
import com.acme.assets.v1.EscalateDepreciationResponse;
import com.acme.assets.v1.ExpireDepreciationRequest;
import com.acme.assets.v1.ExpireDepreciationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.GetDepreciationApprovalStatusRequest;
import com.acme.assets.v1.GetDepreciationApprovalStatusResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ListPendingDepreciationsRequest;
import com.acme.assets.v1.ListPendingDepreciationsResponse;
import com.acme.assets.v1.ListPendingFixedAssetsRequest;
import com.acme.assets.v1.ListPendingFixedAssetsResponse;
import com.acme.assets.v1.RecallDepreciationRequest;
import com.acme.assets.v1.RecallDepreciationResponse;
import com.acme.assets.v1.RejectDepreciationRequest;
import com.acme.assets.v1.RejectDepreciationResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ScheduleAssetTransferExpiryRequest;
import com.acme.assets.v1.ScheduleAssetTransferExpiryResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.SubmitDepreciationRequest;
import com.acme.assets.v1.SubmitDepreciationResponse;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.assets.v1.SuspendDepreciationRequest;
import com.acme.assets.v1.SuspendDepreciationResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.banking.v1.ListStandingOrderWorkflowsRequest;
import com.acme.banking.v1.ListStandingOrderWorkflowsResponse;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseStatusChange;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.experiments.v1.ExperimentQueryServiceGrpc;
import com.acme.experiments.v1.FindExperimentsRequest;
import com.acme.experiments.v1.FindExperimentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.secrets.v1.Secret;
import com.acme.secrets.v1.SecretServiceGrpc;
import com.acme.secrets.v1.UpdateSecretRequest;
import com.acme.secrets.v1.UpdateSecretResponse;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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
 * Routes depreciations through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DepreciationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationApprovalApplication.class);

    private final AssetTransferLifecycleServiceGrpc.AssetTransferLifecycleServiceBlockingStub assetsAssetTransferLifecycleServiceBlocking;

    private final AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking;

    private final DepreciationLifecycleServiceGrpc.DepreciationLifecycleServiceBlockingStub assetsDepreciationLifecycleServiceBlocking;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceFutureStub bankingStandingOrderWorkflowServiceFuture;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExperimentQueryServiceGrpc.ExperimentQueryServiceBlockingStub experimentsExperimentQueryServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final DepreciationApprovalRequestMapper mapper;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    private final SecretServiceGrpc.SecretServiceBlockingStub secretsSecretServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync;

    public DepreciationApprovalApplication(
            AssetTransferLifecycleServiceGrpc.AssetTransferLifecycleServiceBlockingStub assetsAssetTransferLifecycleServiceBlocking,
            AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking,
            DepreciationLifecycleServiceGrpc.DepreciationLifecycleServiceBlockingStub assetsDepreciationLifecycleServiceBlocking,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceFutureStub bankingStandingOrderWorkflowServiceFuture,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            ApplicationEventPublisher events,
            ExperimentQueryServiceGrpc.ExperimentQueryServiceBlockingStub experimentsExperimentQueryServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            DepreciationApprovalRequestMapper mapper,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SecretServiceGrpc.SecretServiceBlockingStub secretsSecretServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync) {
        this.assetsAssetTransferLifecycleServiceBlocking = assetsAssetTransferLifecycleServiceBlocking;
        this.assetsAssetTransferReportingServiceBlocking = assetsAssetTransferReportingServiceBlocking;
        this.assetsDepreciationLifecycleServiceBlocking = assetsDepreciationLifecycleServiceBlocking;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.assetsFixedAssetQueryServiceBlocking = assetsFixedAssetQueryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.bankingStandingOrderWorkflowServiceFuture = bankingStandingOrderWorkflowServiceFuture;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.events = events;
        this.experimentsExperimentQueryServiceBlocking = experimentsExperimentQueryServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.secretsSecretServiceBlocking = secretsSecretServiceBlocking;
        this.treasuryCashPositionServiceAsync = treasuryCashPositionServiceAsync;
    }

    public record SubmitDepreciationStage0Requested(RejectFixedAssetRequest request, CompletableFuture<RejectFixedAssetResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a depreciation.
     */
    public SubmitDepreciationResponse submitDepreciation(SubmitDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = RejectFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectFixedAssetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<RejectFixedAssetResponse>();
        events.publishEvent(new SubmitDepreciationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = BatchCreateCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCashPositionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateCashPositionsResponse.getDefaultInstance());
        var input1 = treasuryCashPositionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCashPositions(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(request.getDepreciation().getOrganizer())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDepreciation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDepreciation().getAudit().getCreatedBy())
                                .setEtag(request.getDepreciation().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitDepreciation", request.getDepreciation().getDepreciationId());
        return SubmitDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply0.getFixedAsset().getAddress().getRecipient())
                                .build())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Approves a depreciation.
     */
    public ApproveDepreciationResponse approveDepreciation(ApproveDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var builder0 = ExpireDepreciationRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDepreciationId(request.getDepreciationId());
        builder0.setDetails(ExpireDepreciationRequest.Details.newBuilder()
                .setNewOwner(UserRef.newBuilder()
                        .setUserId(request.getDepreciation().getOrganizer().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsDepreciationLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expireDepreciation(outbound0);

        var outbound1 = SuspendDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciationId(request.getDepreciation().getDepreciationId())
                .setDetails(SuspendDepreciationRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                                .setUserId(reply0.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .build())
                .build();
        Function<SuspendDepreciationRequest, SuspendDepreciationResponse> operation1 = assetsDepreciationLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suspendDepreciation;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyNoticesRequest.Filter.newBuilder()
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDepreciation().getOrganizer().getEmail())
                        .build())
                .build();
        var stream2 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound2);
        var reply2 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = mapper.approveDepreciationStage3(request, reply1);
        var reply3 = assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingFixedAssets(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getTitle()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveDepreciation", request.getDepreciation().getTitle());
        return ApproveDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getDepreciation().getAudit().getEtag())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(request.getDepreciation().getLocation())
                        .setStatus(reply0.getDepreciation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a depreciation.
     */
    public RejectDepreciationResponse rejectDepreciation(RejectDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ScheduleAssetTransferReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.AssetTransferReportingService");
        var serviceName0 = "acme.assets.v1" + "." + "AssetTransferReportingService";
        var methodName0 = "Sch" + "eduleAssetTransferReport";
        var descriptor0 = MethodDescriptor.<ScheduleAssetTransferReportRequest, ScheduleAssetTransferReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleAssetTransferReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleAssetTransferReportResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = FindExperimentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindExperimentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDepreciation().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        Function<FindExperimentsRequest, FindExperimentsResponse> operation1 = experimentsExperimentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findExperiments;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectDepreciation", request.getDepreciation().getDepreciationId());
        return RejectDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setWindow(request.getDepreciation().getWindow())
                        .build())
                .setOutcome(RejectDepreciationResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Escalates a depreciation.
     */
    public EscalateDepreciationResponse escalateDepreciation(EscalateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ScheduleAssetTransferExpiryRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> assetsAssetTransferLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAssetTransferExpiry(value)).orElseThrow();

        var outbound1 = ListDisposalReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDisposalReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAssetTransfer().getAudit().getCreatedAt())
                        .putAllLabels(request.getDepreciation().getMetadataMap())
                        .setOwnerId(request.getDepreciation().getOrganizer().getUserId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.DisposalReportingService");
        var serviceName1 = "acme.assets.v1" + "." + "DisposalReportingService";
        var methodName1 = "Lis" + "tDisposalReports";
        var descriptor1 = MethodDescriptor.<ListDisposalReportsRequest, ListDisposalReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ListDisposalReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListDisposalReportsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = ListStandingOrderWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListStandingOrderWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAssetTransfer().getDueAt())
                        .setRegion(reply0.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = bankingStandingOrderWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listStandingOrderWorkflows(outbound2);
        var reply2 = RpcSupport.await(pending2);

        List<UpdateSecretResponse> results3 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> UpdateSecretRequest.newBuilder()
                .setContext(request.getContext())
                .setSecret(Secret.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(secretsSecretServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSecret(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? UpdateSecretResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateDepreciation", request.getDepreciation().getDepreciationId());
        return EscalateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .putAllMetadata(reply0.getAssetTransfer().getMetadataMap())
                        .setStatus(request.getDepreciation().getStatus())
                        .build())
                .setOutcome(EscalateDepreciationResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public ListPendingDepreciationsResponse listPendingDepreciations(ListPendingDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "ListPendingDepreciations");

        var outbound0 = RejectCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setExternalReference(request.getFilter().getOwnerId())
                        .addHistory(CollectionCaseStatusChange.newBuilder()
                                .setChangedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCollectionCase(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingDepreciations", request.getPage().getPageToken());
        return ListPendingDepreciationsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a depreciation.
     */
    public DelegateDepreciationApprovalResponse delegateDepreciationApproval(DelegateDepreciationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateDepreciationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = JsonCodec.roundTrip(SummarizeFixedAssetsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeFixedAssetsRequest.Details.newBuilder()
                        .setPeriod(request.getDepreciation().getWindow())
                        .build())
                .setReason(request.getReason())
                .build(), SummarizeFixedAssetsRequest.class);
        var reply0 = assetsFixedAssetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeFixedAssets(outbound0);

        var outbound1 = SendRetentionPolicyTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendRetentionPolicyTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.RetentionPolicyNotificationService");
        var selected1 = RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.sendRetentionPolicyTestNotice(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateDepreciationApproval", request.getDepreciation().getDepreciationId());
        return DelegateDepreciationApprovalResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(request.getDepreciation().getOrganizer())
                        .build())
                .setOutcome(DelegateDepreciationApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a depreciation.
     */
    public RecallDepreciationResponse recallDepreciation(RecallDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(outbound0), rpcExecutor);

        var outbound1 = CountAssetTransferTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAssetTransferTotalsRequest.Filter.newBuilder()
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAssetTransferTotals(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallDepreciation", request.getDepreciation().getDepreciationId());
        return RecallDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .build())
                .setOutcome(RecallDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public GetDepreciationApprovalStatusResponse getDepreciationApprovalStatus(GetDepreciationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDepreciationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "GetDepreciationApprovalStatus");

        var outbound0 = RetryRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryRiskScenarioWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setSubject(request.getDepreciationId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskScenarioWorkflowService");
        var selected0 = RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.retryRiskScenarioWorkflow(outbound0);
        return GetDepreciationApprovalStatusResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }
}
