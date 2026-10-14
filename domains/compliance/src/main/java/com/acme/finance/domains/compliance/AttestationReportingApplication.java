package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.CalculateControlRequest;
import com.acme.compliance.v1.CalculateControlResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DeleteAttestationReportRequest;
import com.acme.compliance.v1.DeleteAttestationReportResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.FetchAttestationReportRequest;
import com.acme.compliance.v1.FetchAttestationReportResponse;
import com.acme.compliance.v1.GenerateAttestationReportRequest;
import com.acme.compliance.v1.GenerateAttestationReportResponse;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.StreamAttestationMetricsRequest;
import com.acme.compliance.v1.StreamAttestationMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.SignalClaimWorkflowRequest;
import com.acme.insurance.v1.SignalClaimWorkflowResponse;
import com.acme.insurance.v1.Underwriting;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Builds and schedules reports about attestations. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class AttestationReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationReportingApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceFutureStub complianceComplianceCaseCalculationServiceFuture;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final ComplianceRepository repository;

    public AttestationReportingApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceFutureStub complianceComplianceCaseCalculationServiceFuture,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking,
            ApplicationEventPublisher events,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            ComplianceRepository repository) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.complianceComplianceCaseCalculationServiceFuture = complianceComplianceCaseCalculationServiceFuture;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceSanctionCalculationServiceBlocking = complianceSanctionCalculationServiceBlocking;
        this.events = events;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.repository = repository;
    }

    public record CountAttestationTotalsStage0Requested(RepairAuditTrailRequest request, CompletableFuture<RepairAuditTrailResponse> completion, io.grpc.Context context) {}

    /**
     * Generates an attestation.
     */
    public GenerateAttestationReportResponse generateAttestationReport(GenerateAttestationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateAttestationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "GenerateAttestationReport");

        var outbound0 = JsonCodec.roundTrip(ResumePaymentPromiseWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build(), ResumePaymentPromiseWorkflowRequest.class);
        var reply0 = collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumePaymentPromiseWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateAttestationReport", request.getAttestationId());
        return GenerateAttestationReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public FetchAttestationReportResponse fetchAttestationReport(FetchAttestationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchAttestationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "FetchAttestationReport");

        var outbound0 = PreviewComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setAttestationId(request.getAttestationId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewComplianceCaseRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<PreviewComplianceCaseRequest, PreviewComplianceCaseResponse>() {
            @Override
            public PreviewComplianceCaseResponse apply(PreviewComplianceCaseRequest input) {
                return complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewComplianceCase(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return FetchAttestationReportResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(reply0.getComplianceCase().getCustomer().getAttestationId())
                        .build())
                .build();
    }

    /**
     * Schedules an attestation.
     */
    public ScheduleAttestationReportResponse scheduleAttestationReport(ScheduleAttestationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleAttestationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "ScheduleAttestationReport");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateControlRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setBlock(ControlBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), CalculateControlRequest.class);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateControl(outbound0);

        var outbound1 = NotifyAuditEventWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditEventWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditEventWatchers(value));
        log.atDebug().addKeyValue("auditValue", request.getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleAttestationReport", request.getAttestationId());
        return ScheduleAttestationReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of attestations as they happen.
     */
    public StreamAttestationMetricsResponse streamAttestationMetrics(StreamAttestationMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAttestationMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getContext().getTraceId(), "StreamAttestationMetrics");

        var outbound0 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainComplianceCaseCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = complianceComplianceCaseCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAttestationMetrics", request.getFilter().getQuery());
        return StreamAttestationMetricsResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getComplianceCase().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists attestations a page at a time.
     */
    public ListAttestationReportsResponse listAttestationReports(ListAttestationReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAttestationReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getContext().getTraceId(), "ListAttestationReports");

        var outbound0 = FindAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccessPoliciesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAccessPolicies(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAttestationReports", request.getPage().getPageToken());
        return ListAttestationReportsResponse.newBuilder()
                .addAttestations(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getAuditTrail().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(reply1.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Shares an attestation.
     */
    public ShareAttestationReportResponse shareAttestationReport(ShareAttestationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareAttestationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "ShareAttestationReport");

        var outbound0 = CalculateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getRecipient().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setUserId(request.getContext().getActor().getUserId())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<CalculateSanctionRequest, CalculateSanctionResponse> port0 = RpcPort.adapt(complianceSanctionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateSanction);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ReconcileUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSanction().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.UnderwritingSyncService");
        var serviceName1 = "acme.insurance.v1" + "." + "UnderwritingSyncService";
        var methodName1 = "Rec" + "oncileUnderwritings";
        var descriptor1 = MethodDescriptor.<ReconcileUnderwritingsRequest, ReconcileUnderwritingsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ReconcileUnderwritingsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ReconcileUnderwritingsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(SignalClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(reply1.getUnderwriting().getRequester().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getSanction().getManager().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), SignalClaimWorkflowRequest.class);
        var reply2 = insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalClaimWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareAttestationReport", request.getDetails().getRecipient().getPhone());
        return ShareAttestationReportResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getClaim().getAudit().getEtag())
                                .setUpdatedBy(reply0.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getSanction().getLocation().getRegion())
                                .build())
                        .setDueAt(reply1.getUnderwriting().getSla().getDueAt())
                        .build())
                .setOutcome(ShareAttestationReportResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Counts attestations by status.
     */
    public CountAttestationTotalsResponse countAttestationTotals(CountAttestationTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountAttestationTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getContext().getTraceId(), "CountAttestationTotals");

        var outbound0 = RepairAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RepairAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var completion0 = new CompletableFuture<RepairAuditTrailResponse>();
        events.publishEvent(new CountAttestationTotalsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountAttestationTotals", request.getPage().getPageToken());
        return CountAttestationTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes an attestation.
     */
    public DeleteAttestationReportResponse deleteAttestationReport(DeleteAttestationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteAttestationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "DeleteAttestationReport");

        var outbound0 = RecalculateControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<RecalculateControlsRequest, RecalculateControlsResponse> port0 = RpcPort.adapt(complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateControls);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = EscalatePaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .setUserId(reply0.getControl().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.PaymentPromiseApprovalService");
        var serviceName1 = "acme.collections.v1" + "." + "PaymentPromiseApprovalService";
        var methodName1 = "Esc" + "alatePaymentPromise";
        var descriptor1 = MethodDescriptor.<EscalatePaymentPromiseRequest, EscalatePaymentPromiseResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(EscalatePaymentPromiseRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EscalatePaymentPromiseResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteAttestationReport", request.getAttestationId());
        return DeleteAttestationReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
