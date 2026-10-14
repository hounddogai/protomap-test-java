package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.ExportDepreciationsRequest;
import com.acme.assets.v1.ExportDepreciationsResponse;
import com.acme.assets.v1.FindFixedAssetsRequest;
import com.acme.assets.v1.FindFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.PushAssetTransferRequest;
import com.acme.assets.v1.PushAssetTransferResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalRequest;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Keeps asset transfers in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AssetTransferSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferSyncApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public AssetTransferSyncApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.assetsFixedAssetQueryServiceBlocking = assetsFixedAssetQueryServiceBlocking;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.complianceComplianceCaseApprovalServiceBlocking = complianceComplianceCaseApprovalServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Syncs an asset transfer.
     */
    public SyncAssetTransfersResponse syncAssetTransfers(SyncAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = JsonCodec.roundTrip(CheckRetentionPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .setUpdatedBy(request.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CheckRetentionPolicyEligibilityRequest.class);
        var reply0 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRetentionPolicyEligibility(outbound0);

        var outbound1 = CompareDisposalOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareDisposalOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getAssetTransfer().getBillingAddress().getPostalCode())
                                .setRecipient(request.getAssetTransfer().getBillingAddress().getRecipient())
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDisposalOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncAssetTransfers", request.getAssetTransfer().getAssetTransferId());
        return SyncAssetTransfersResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getAssetTransfer().getBillingAddress())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setOutcome(SyncAssetTransfersResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Pushs an asset transfer.
     */
    public PushAssetTransferResponse pushAssetTransfer(PushAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RpcSupport.unpack(Any.pack(ValidateDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getAssetTransfer().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()), ValidateDepreciationRequest.class);
        var reply0 = assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDepreciation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return PushAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .setIssuedAt(request.getAssetTransfer().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Pulls an asset transfer.
     */
    public PullAssetTransfersResponse pullAssetTransfers(PullAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RejectPaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RejectPaymentPromiseResponse fetch(RejectPaymentPromiseRequest input) {
                return collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectPaymentPromise(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = DelegateComplianceCaseApprovalRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPaymentPromise().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<DelegateComplianceCaseApprovalRequest, DelegateComplianceCaseApprovalResponse> operation1 = complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::delegateComplianceCaseApproval;
        var reply1 = operation1.apply(outbound1);

        var futures2 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> FindFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindFixedAssetsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply0.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findFixedAssets(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? FindFixedAssetsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = ExportDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportDepreciationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getAudit().getCreatedAt())
                        .setRegion(reply0.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var stream3 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportDepreciations(outbound3);
        var reply3 = ExportDepreciationsResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullAssetTransfers", request.getAssetTransfer().getAssetTransferId());
        return PullAssetTransfersResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setNotes(request.getAssetTransfer().getNotes())
                        .build())
                .setOutcome(PullAssetTransfersResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Reconciles an asset transfer.
     */
    public ReconcileAssetTransfersResponse reconcileAssetTransfers(ReconcileAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ListFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFixedAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getIssuedAt())
                        .putAllLabels(request.getAssetTransfer().getMetadataMap())
                        .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.FixedAssetService");
        var serviceName0 = "acme.assets.v1" + "." + "FixedAssetService";
        var methodName0 = "Lis" + "tFixedAssets";
        var descriptor0 = MethodDescriptor.<ListFixedAssetsRequest, ListFixedAssetsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ListFixedAssetsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListFixedAssetsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = RecallLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getAssetTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setLast4(switch (request.getAssetTransfer().getSettlementCase()) {
                                    case CARD -> request.getAssetTransfer().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallLoanApplication(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileAssetTransfers", request.getAssetTransfer().getAssetTransferId());
        return ReconcileAssetTransfersResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(request.getAssetTransfer().getBillingAddress())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public GetAssetTransferSyncStateResponse getAssetTransferSyncState(GetAssetTransferSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAssetTransferSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GetAssetTransferSyncState");

        var outbound0 = BatchGetFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.FixedAssetService");
        var selected0 = FixedAssetServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.batchGetFixedAssets(outbound0);
        return GetAssetTransferSyncStateResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists asset transfers a page at a time.
     */
    public ListAssetTransferConflictsResponse listAssetTransferConflicts(ListAssetTransferConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAssetTransferConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "ListAssetTransferConflicts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAssetTransferConflicts", request.getPage().getPageToken());
        return ListAssetTransferConflictsResponse.getDefaultInstance();
    }
}
