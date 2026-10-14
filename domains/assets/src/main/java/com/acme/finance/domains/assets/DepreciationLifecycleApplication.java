package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDepreciationRequest;
import com.acme.assets.v1.ActivateDepreciationResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CountDepreciationsRequest;
import com.acme.assets.v1.CountDepreciationsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.ExpireDepreciationRequest;
import com.acme.assets.v1.ExpireDepreciationResponse;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetDepreciationLifecycleRequest;
import com.acme.assets.v1.GetDepreciationLifecycleResponse;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.assets.v1.PushAssetTransferRequest;
import com.acme.assets.v1.PushAssetTransferResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.ReinstateDepreciationRequest;
import com.acme.assets.v1.ReinstateDepreciationResponse;
import com.acme.assets.v1.RenewDepreciationRequest;
import com.acme.assets.v1.RenewDepreciationResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.SuspendDepreciationRequest;
import com.acme.assets.v1.SuspendDepreciationResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitBlockAction;
import com.acme.ratelimiting.v1.RateLimitServiceGrpc;
import com.acme.ratelimiting.v1.UpdateRateLimitRequest;
import com.acme.ratelimiting.v1.UpdateRateLimitResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves a depreciation between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class DepreciationLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationLifecycleApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking;

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync;

    private final RateLimitServiceGrpc.RateLimitServiceFutureStub ratelimitingRateLimitServiceFuture;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public DepreciationLifecycleApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking,
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync,
            RateLimitServiceGrpc.RateLimitServiceFutureStub ratelimitingRateLimitServiceFuture,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.assetsDisposalSyncServiceBlocking = assetsDisposalSyncServiceBlocking;
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.invoicingInvoiceBatchServiceAsync = invoicingInvoiceBatchServiceAsync;
        this.ratelimitingRateLimitServiceFuture = ratelimitingRateLimitServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Activates a depreciation.
     */
    public ActivateDepreciationResponse activateDepreciation(ActivateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "ActivateDepreciation");

        var outbound0 = PushAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PushAssetTransferRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<PushAssetTransferRequest, PushAssetTransferResponse>() {
            @Override
            public PushAssetTransferResponse apply(PushAssetTransferRequest input) {
                return assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushAssetTransfer(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ListAssetTransferConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAssetTransferConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAssetTransfer().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setRegion(reply0.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.AssetTransferSyncService");
        var serviceName1 = "acme.assets.v1" + "." + "AssetTransferSyncService";
        var methodName1 = "Lis" + "tAssetTransferConflicts";
        var descriptor1 = MethodDescriptor.<ListAssetTransferConflictsRequest, ListAssetTransferConflictsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ListAssetTransferConflictsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListAssetTransferConflictsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = BatchCreateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateInvoiceBatchesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateInvoiceBatchesResponse.getDefaultInstance());
        var input2 = invoicingInvoiceBatchServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateInvoiceBatches(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(request.getDetails().getNewOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setAuthorEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input3 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateDepreciation", request.getDetails().getReasonCode());
        return ActivateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAssetTransfer().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getAssetTransfer().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setOutcome(ActivateDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Suspends a depreciation.
     */
    public SuspendDepreciationResponse suspendDepreciation(SuspendDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "SuspendDepreciation");

        var builder0 = EstimateDisposalRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDisposal(Disposal.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setUserId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDisposal(outbound0);

        var outbound1 = ValidateDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getDisposal().getAssignee().getEmail())
                                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ValidateDepreciationRequest, ValidateDepreciationResponse> port1 = RpcPort.adapt(assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateDepreciation);
        var reply1 = port1.exchange(outbound1);

        var builder2 = ReconcileDisposalsRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(ReconcileDisposalsRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder2.setDisposal(Disposal.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setEmail(reply0.getDisposal().getRequester().getEmail())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(reply0.getDisposal().getAudit().getCreatedAt())
                        .build())
                .setCustomer(FixedAssetRef.newBuilder()
                        .setDisplayName(reply1.getDepreciation().getOrganizer().getDisplayName())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = assetsDisposalSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileDisposals(outbound2);

        var outbound3 = UpdateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getDisposal().getAudit().getEtag())
                                .setUpdatedBy(reply1.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getDisposal().getAssignee().getEmail())
                                .setUserId(reply0.getDisposal().getAssignee().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.authorization.v1.AccessPolicyService");
        var serviceName3 = "acme.authorization.v1" + "." + "AccessPolicyService";
        var methodName3 = "Upd" + "ateAccessPolicy";
        var descriptor3 = MethodDescriptor.<UpdateAccessPolicyRequest, UpdateAccessPolicyResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(UpdateAccessPolicyRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(UpdateAccessPolicyResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendDepreciation", request.getDetails().getReasonCode());
        return SuspendDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDepreciation().getAudit().getCreatedBy())
                                .setEtag(reply2.getDisposal().getAudit().getEtag())
                                .setUpdatedBy(reply3.getAccessPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply3.getAccessPolicy().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .setOutcome(SuspendDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Reinstates a depreciation.
     */
    public ReinstateDepreciationResponse reinstateDepreciation(ReinstateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "ReinstateDepreciation");

        var outbound0 = ListDepreciationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationReports(outbound0), rpcExecutor);

        var outbound1 = SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateDepreciation", request.getDetails().getReasonCode());
        return ReinstateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .build())
                .setOutcome(ReinstateDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a depreciation.
     */
    public RenewDepreciationResponse renewDepreciation(RenewDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "RenewDepreciation");

        var outbound0 = ListSanctionReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSanctionReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        final class Stage0Adapter {
            ListSanctionReportsResponse fetch(ListSanctionReportsRequest input) {
                return complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSanctionReports(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = UpdateRateLimitRequest.newBuilder()
                .setContext(request.getContext())
                .setRateLimit(RateLimit.newBuilder()
                        .setBlock(RateLimitBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = ratelimitingRateLimitServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).updateRateLimit(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewDepreciation", request.getDetails().getReasonCode());
        return RenewDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .build())
                .setOutcome(RenewDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Expires a depreciation.
     */
    public ExpireDepreciationResponse expireDepreciation(ExpireDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "ExpireDepreciation");

        var outbound0 = ShareDepreciationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareDepreciationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDepreciationReport(value));

        var outbound1 = BeanCopies.copy(ScheduleFixedAssetExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleFixedAssetExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build(), ScheduleFixedAssetExpiryRequest.class);
        var reply1 = assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFixedAssetExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireDepreciation", request.getDetails().getReasonCode());
        return ExpireDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getFixedAsset().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .setOutcome(ExpireDepreciationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public GetDepreciationLifecycleResponse getDepreciationLifecycle(GetDepreciationLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDepreciationLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "GetDepreciationLifecycle");

        var outbound0 = CountDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountDepreciationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getDepreciationId())
                        .build())
                .build();
        final class Stage0Adapter {
            CountDepreciationsResponse fetch(CountDepreciationsRequest input) {
                return assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countDepreciations(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = PullPremiumsRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(PremiumLine.newBuilder()
                                .putAllAttributes(reply0.getResult().getDetailsMap())
                                .build())
                        .build())
                .build();
        Function<PullPremiumsRequest, PullPremiumsResponse> operation1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pullPremiums;
        var reply1 = operation1.apply(outbound1);
        return GetDepreciationLifecycleResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getPremium().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
