package com.acme.finance.domains.assets;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.DeleteFixedAssetRequest;
import com.acme.assets.v1.DeleteFixedAssetResponse;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetFixedAssetRequest;
import com.acme.assets.v1.GetFixedAssetResponse;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.assets.v1.LookupFixedAssetRequest;
import com.acme.assets.v1.LookupFixedAssetResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.SuggestDepreciationsRequest;
import com.acme.assets.v1.SuggestDepreciationsResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.maintenance.v1.ListWorkOrdersRequest;
import com.acme.maintenance.v1.ListWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrderServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Manages fixed assets: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FixedAssetApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetApplication.class);

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final WorkOrderServiceGrpc.WorkOrderServiceBlockingStub maintenanceWorkOrderServiceBlocking;

    private final AssetsRepository repository;

    public FixedAssetApplication(
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            WorkOrderServiceGrpc.WorkOrderServiceBlockingStub maintenanceWorkOrderServiceBlocking,
            AssetsRepository repository) {
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.maintenanceWorkOrderServiceBlocking = maintenanceWorkOrderServiceBlocking;
        this.repository = repository;
    }

    /**
     * Creates a fixed asset.
     */
    public CreateFixedAssetResponse createFixedAsset(CreateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = LintDisposalsRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<LintDisposalsRequest, LintDisposalsResponse> operation0 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintDisposals;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound1);
        var reply1 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDisposal().getRequester().getEmail())
                                .setPreferredChannel(reply0.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(outbound2);

        var outbound3 = RecalculateAttestationsRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getPostalCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAttestations(outbound3);

        var outbound4 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input4 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFixedAsset().getAudit().getCreatedBy())
                                .setEtag(request.getFixedAsset().getAudit().getEtag())
                                .setUpdatedAt(request.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateFixedAsset", request.getFixedAsset().getFixedAssetId());
        return CreateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getMandate().getAudit().getEtag())
                                .setUpdatedBy(reply3.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setCode(reply0.getDisposal().getCustomer().getCode())
                        .setOwner(request.getFixedAsset().getOwner())
                        .putAllSpecs(request.getFixedAsset().getSpecsMap())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public GetFixedAssetResponse getFixedAsset(GetFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "GetFixedAsset");

        var outbound0 = ListWorkOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListWorkOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getFixedAssetId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(maintenanceWorkOrderServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listWorkOrders(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = LookupFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAssetId(request.getFixedAssetId())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.FixedAssetQueryService");
        var serviceName1 = "acme.assets.v1" + "." + "FixedAssetQueryService";
        var methodName1 = "Loo" + "kupFixedAsset";
        var descriptor1 = MethodDescriptor.<LookupFixedAssetRequest, LookupFixedAssetResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(LookupFixedAssetRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(LookupFixedAssetResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        return GetFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setStatus(reply1.getFixedAsset().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Deletes a fixed asset.
     */
    public DeleteFixedAssetResponse deleteFixedAsset(DeleteFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "DeleteFixedAsset");

        var outbound0 = QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.CoverageCalculationService");
        var selected0 = CoverageCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quoteCoverage(outbound0);

        var outbound1 = JsonCodec.roundTrip(ValidateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCoverage().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .build(), ValidateLedgerAccountRequest.class);
        var reply1 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLedgerAccount(outbound1);

        var outbound2 = SuggestDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestDepreciationsRequest.Filter.newBuilder()
                        .putAllLabels(reply1.getLedgerAccount().getLabelsMap())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.assets.v1.DepreciationQueryService");
        var selected2 = DepreciationQueryServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.suggestDepreciations(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteFixedAsset", request.getFixedAssetId());
        return DeleteFixedAssetResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public ListFixedAssetsResponse listFixedAssets(ListFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "ListFixedAssets");

        var outbound0 = ScoreAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAccessPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.authorization.v1.AccessPolicyValidationService");
        var selected0 = AccessPolicyValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scoreAccessPolicy(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFixedAssets", request.getPage().getPageToken());
        return ListFixedAssetsResponse.newBuilder()
                .addFixedAssets(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAccessPolicy().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns several fixed assets at once.
     */
    public BatchGetFixedAssetsResponse batchGetFixedAssets(BatchGetFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "BatchGetFixedAssets");

        var outbound0 = LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFixedAssetsRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFixedAssets(outbound0);
        return BatchGetFixedAssetsResponse.newBuilder()
                .addFixedAssets(FixedAsset.newBuilder()
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setHeight(reply0.getFixedAsset().getDimensions().getHeight())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public SearchFixedAssetsResponse searchFixedAssets(SearchFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "SearchFixedAssets");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchFixedAssets", request.getPage().getPageToken());
        return SearchFixedAssetsResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of fixed assets as they happen.
     */
    public WatchFixedAssetsResponse watchFixedAssets(WatchFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "WatchFixedAssets");

        var outbound0 = ValidateDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateDisposalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDisposal(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchFixedAssets", request.getFilter().getQuery());
        return WatchFixedAssetsResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Takes a stream of fixed assets and answers once at the end.
     */
    public BatchCreateFixedAssetsResponse batchCreateFixedAssets(BatchCreateFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveFixedAsset(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getCreatedBy();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(outbound0);

        var outbound1 = LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getMandate().getBillingAddress().getLocality())
                                .build())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription();
                                    default -> "";
                                })
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFixedAssets(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateFixedAssets", request.getHeader().getSource());
        return BatchCreateFixedAssetsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
