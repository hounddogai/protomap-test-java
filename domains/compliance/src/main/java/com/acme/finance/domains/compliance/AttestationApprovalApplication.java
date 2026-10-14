package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.compliance.v1.EscalateAttestationRequest;
import com.acme.compliance.v1.EscalateAttestationResponse;
import com.acme.compliance.v1.GetAttestationApprovalStatusRequest;
import com.acme.compliance.v1.GetAttestationApprovalStatusResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RenewAttestationRequest;
import com.acme.compliance.v1.RenewAttestationResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.SubmitAttestationRequest;
import com.acme.compliance.v1.SubmitAttestationResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumePolicyWorkflowRequest;
import com.acme.insurance.v1.ResumePolicyWorkflowResponse;
import com.acme.insurance.v1.StartCoverageWorkflowRequest;
import com.acme.insurance.v1.StartCoverageWorkflowResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.EscalateRepaymentRequest;
import com.acme.lending.v1.EscalateRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.search.v1.CreateSearchIndexRequest;
import com.acme.search.v1.CreateSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes attestations through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AttestationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationApprovalApplication.class);

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final AttestationLifecycleServiceGrpc.AttestationLifecycleServiceBlockingStub complianceAttestationLifecycleServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceFutureStub ledgerFiscalPeriodNotificationServiceFuture;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final AttestationApprovalRequestMapper mapper;

    private final ComplianceRepository repository;

    private final ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public AttestationApprovalApplication(
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            AttestationLifecycleServiceGrpc.AttestationLifecycleServiceBlockingStub complianceAttestationLifecycleServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            ApplicationEventPublisher events,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceFutureStub ledgerFiscalPeriodNotificationServiceFuture,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            AttestationApprovalRequestMapper mapper,
            ComplianceRepository repository,
            ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.complianceAttestationLifecycleServiceBlocking = complianceAttestationLifecycleServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.events = events;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceFuture = ledgerFiscalPeriodNotificationServiceFuture;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureApprovalServiceBlocking = riskExposureApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    public record CommentAttestationApprovalStage0Requested(SummarizeComplianceCasesRequest request, CompletableFuture<SummarizeComplianceCasesResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an attestation.
     */
    public SubmitAttestationResponse submitAttestation(SubmitAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CompareSanctionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getAttestation().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(request.getAttestation().getBillingAddress().getRecipient())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(request.getAttestation().getBillingAddress().getLinesList())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.SanctionCalculationService");
        var selected0 = SanctionCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.compareSanctionOptions(outbound0);

        var outbound1 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getAttestation().getAttestationId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input1 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .setUpdatedAt(request.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitAttestation", request.getAttestation().getNumber());
        return SubmitAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSanction().getAudit().getCreatedBy())
                                .build())
                        .setStatus(request.getAttestation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves an attestation.
     */
    public ApproveAttestationResponse approveAttestation(ApproveAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = EscalateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateRepayment(outbound0), rpcExecutor);

        var outbound1 = CountAttestationTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAttestationTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAttestation().getAudit().getUpdatedAt())
                        .setRegion(request.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAttestationTotals(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = mapper.approveAttestationStage2(request, reply0);
        var reply2 = riskExposureApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitExposure(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveAttestation", request.getAttestation().getAttestationId());
        return ApproveAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setStatus(request.getAttestation().getStatus())
                        .build())
                .setOutcome(ApproveAttestationResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects an attestation.
     */
    public RejectAttestationResponse rejectAttestation(RejectAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = SubmitRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitRepayment(outbound0), rpcExecutor);

        var outbound1 = ScheduleSanctionReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleSanctionReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ListFiscalPeriodSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getRepayment().getAudit().getUpdatedAt())
                        .setRegion(reply0.getRepayment().getLocation().getRegion())
                        .build())
                .build();
        var pending2 = ledgerFiscalPeriodNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listFiscalPeriodSubscriptions(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = ResumePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply3 = insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumePolicyWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectAttestation", request.getAttestation().getAttestationId());
        return RejectAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setBillingAddress(request.getAttestation().getBillingAddress())
                        .build())
                .setOutcome(RejectAttestationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates an attestation.
     */
    public EscalateAttestationResponse escalateAttestation(EscalateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CreateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getAttestation().getBillingAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAttestation().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.search.v1.SearchIndexService/CreateSearchIndex", outbound0);
        var reply0 = searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSearchIndex(command0.payload());

        var outbound1 = ExportAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAuditTrailsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAttestation().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getSearchIndex().getSearchIndexId())
                        .setRegion(request.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAuditTrails(outbound1);
        var reply1 = ExportAuditTrailsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateAttestation", request.getAttestation().getAttestationId());
        return EscalateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .setOutcome(EscalateAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Delegates an attestation.
     */
    public DelegateAttestationApprovalResponse delegateAttestationApproval(DelegateAttestationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateAttestationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = RenewAttestationRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAttestationId(request.getAttestationId())
                .setContext(request.getContext())
                .setDetails(RenewAttestationRequest.Details.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceAttestationLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).renewAttestation(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateAttestationApproval", request.getAttestation().getAttestationId());
        return DelegateAttestationApprovalResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setNotes(request.getAttestation().getNotes())
                        .setTotal(reply0.getAttestation().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public GetAttestationApprovalStatusResponse getAttestationApprovalStatus(GetAttestationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAttestationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "GetAttestationApprovalStatus");

        var outbound0 = ScheduleControlExpiryRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ControlLifecycleService");
        var serviceName0 = "acme.compliance.v1" + "." + "ControlLifecycleService";
        var methodName0 = "Sch" + "eduleControlExpiry";
        var descriptor0 = MethodDescriptor.<ScheduleControlExpiryRequest, ScheduleControlExpiryResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleControlExpiryRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleControlExpiryResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return GetAttestationApprovalStatusResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Comments an attestation.
     */
    public CommentAttestationApprovalResponse commentAttestationApproval(CommentAttestationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentAttestationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = SummarizeComplianceCasesRequest.newBuilder()
                .setComplianceCaseId(request.getAttestation().getCounterparty().getComplianceCaseId())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<SummarizeComplianceCasesResponse>();
        events.publishEvent(new CommentAttestationApprovalStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = StartCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setDetails(StartCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startCoverageWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentAttestationApproval", request.getAttestation().getAttestationId());
        return CommentAttestationApprovalResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setTotal(request.getAttestation().getTotal())
                        .build())
                .setOutcome(CommentAttestationApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
