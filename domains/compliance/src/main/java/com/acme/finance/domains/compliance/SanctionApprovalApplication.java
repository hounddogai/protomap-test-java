package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceControlWorkflowRequest;
import com.acme.compliance.v1.AdvanceControlWorkflowResponse;
import com.acme.compliance.v1.ApproveSanctionRequest;
import com.acme.compliance.v1.ApproveSanctionResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.CommentComplianceCaseApprovalRequest;
import com.acme.compliance.v1.CommentComplianceCaseApprovalResponse;
import com.acme.compliance.v1.CommentSanctionApprovalRequest;
import com.acme.compliance.v1.CommentSanctionApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.DelegateSanctionApprovalRequest;
import com.acme.compliance.v1.DelegateSanctionApprovalResponse;
import com.acme.compliance.v1.EscalateSanctionRequest;
import com.acme.compliance.v1.EscalateSanctionResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GetSanctionApprovalStatusRequest;
import com.acme.compliance.v1.GetSanctionApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingSanctionsRequest;
import com.acme.compliance.v1.ListPendingSanctionsResponse;
import com.acme.compliance.v1.RecallSanctionRequest;
import com.acme.compliance.v1.RecallSanctionResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.compliance.v1.SubmitSanctionRequest;
import com.acme.compliance.v1.SubmitSanctionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.forms.v1.BatchGetFormsRequest;
import com.acme.forms.v1.BatchGetFormsResponse;
import com.acme.forms.v1.FormServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountLimit;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.shipping.v1.ShipmentQueryServiceGrpc;
import com.acme.shipping.v1.SuggestShipmentsRequest;
import com.acme.shipping.v1.SuggestShipmentsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes sanctions through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class SanctionApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(SanctionApprovalApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final ChannelRegistry channels;

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final ControlLifecycleServiceGrpc.ControlLifecycleServiceBlockingStub complianceControlLifecycleServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final SanctionApprovalRequestMapper mapper;

    private final ComplianceRepository repository;

    private final ShipmentQueryServiceGrpc.ShipmentQueryServiceBlockingStub shippingShipmentQueryServiceBlocking;

    public SanctionApprovalApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            ChannelRegistry channels,
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            ControlLifecycleServiceGrpc.ControlLifecycleServiceBlockingStub complianceControlLifecycleServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            SanctionApprovalRequestMapper mapper,
            ComplianceRepository repository,
            ShipmentQueryServiceGrpc.ShipmentQueryServiceBlockingStub shippingShipmentQueryServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.channels = channels;
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.complianceComplianceCaseApprovalServiceBlocking = complianceComplianceCaseApprovalServiceBlocking;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.complianceControlLifecycleServiceBlocking = complianceControlLifecycleServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.shippingShipmentQueryServiceBlocking = shippingShipmentQueryServiceBlocking;
    }

    /**
     * Submits a sanction.
     */
    public SubmitSanctionResponse submitSanction(SubmitSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = AdvanceControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ControlWorkflowService");
        var serviceName0 = "acme.compliance.v1" + "." + "ControlWorkflowService";
        var methodName0 = "Adv" + "anceControlWorkflow";
        var descriptor0 = MethodDescriptor.<AdvanceControlWorkflowRequest, AdvanceControlWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(AdvanceControlWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(AdvanceControlWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CommentRepaymentApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentRepaymentApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                                .setEmail(request.getSanction().getManager().getEmail())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .build();
        Function<CommentRepaymentApprovalRequest, CommentRepaymentApprovalResponse> operation1 = lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::commentRepaymentApproval;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = CommentComplianceCaseApprovalRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getControl().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentComplianceCaseApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.compliance.v1.ComplianceCaseApprovalService");
        var serviceName2 = "acme.compliance.v1" + "." + "ComplianceCaseApprovalService";
        var methodName2 = "Com" + "mentComplianceCaseApproval";
        var descriptor2 = MethodDescriptor.<CommentComplianceCaseApprovalRequest, CommentComplianceCaseApprovalResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CommentComplianceCaseApprovalRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CommentComplianceCaseApprovalResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input3 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .setCreatedBy(request.getSanction().getAudit().getCreatedBy())
                                .setEtag(request.getSanction().getAudit().getEtag())
                                .setUpdatedAt(request.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitSanction", request.getSanction().getSanctionId());
        return SubmitSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getComplianceCase().getAudit().getEtag())
                                .build())
                        .addChildren(Sanction.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply2.getComplianceCase().getAssignee().getDisplayName())
                                        .build())
                                .setName(reply0.getControl().getName())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply1.getRepayment().getLocation().getLinesList())
                                .setRecipient(reply1.getRepayment().getLocation().getRecipient())
                                .build())
                        .setManager(request.getSanction().getManager())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Approves a sanction.
     */
    public ApproveSanctionResponse approveSanction(ApproveSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = ListPremiumConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPremiumConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getSanction().getManager().getUserId())
                        .setRegion(request.getSanction().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPremiumConflicts(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveSanction", request.getSanction().getSanctionId());
        return ApproveSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setLocation(request.getSanction().getLocation())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Rejects a sanction.
     */
    public RejectSanctionResponse rejectSanction(RejectSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = RejectAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSanction().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<RejectAttestationRequest, RejectAttestationResponse>() {
            @Override
            public RejectAttestationResponse apply(RejectAttestationRequest input) {
                return complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectAttestation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ScheduleControlExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAttestation().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceControlLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleControlExpiry(value));

        var outbound2 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSanction().getAudit().getCreatedBy())
                                .setEtag(request.getSanction().getAudit().getEtag())
                                .setUpdatedAt(reply0.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .addLimits(LedgerAccountLimit.newBuilder()
                                .setAmount(reply1.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<AnnotateLedgerAccountVersionRequest, AnnotateLedgerAccountVersionResponse>() {
            @Override
            public AnnotateLedgerAccountVersionResponse apply(AnnotateLedgerAccountVersionRequest input) {
                return ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectSanction", request.getSanction().getSanctionId());
        return RejectSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(reply1.getControl().getAudit().getUpdatedAt())
                                        .setUpdatedBy(reply2.getLedgerAccount().getAudit().getUpdatedBy())
                                        .build())
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply0.getAttestation().getAudit().getEtag())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getAttestation().getBillingAddress().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .setStatus(request.getSanction().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a sanction.
     */
    public EscalateSanctionResponse escalateSanction(EscalateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = (ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAuditEventRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getSanction().getLocation().getCountryCode())
                                .setRegion(request.getSanction().getLocation().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateSanction", request.getSanction().getSanctionId());
        return EscalateSanctionResponse.newBuilder()
                .setOutcome(EscalateSanctionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .putAllAttributes(reply0.getAuditEvent().getAttributesMap())
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Lists sanctions a page at a time.
     */
    public ListPendingSanctionsResponse listPendingSanctions(ListPendingSanctionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingSanctions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getContext().getTraceId(), "ListPendingSanctions");

        var outbound0 = SuggestShipmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestShipmentsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var reply0 = shippingShipmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestShipments(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingSanctions", request.getFilter().getOwnerId());
        return ListPendingSanctionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a sanction.
     */
    public DelegateSanctionApprovalResponse delegateSanctionApproval(DelegateSanctionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateSanctionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = BatchGetFormsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.forms.v1.FormService");
        var selected0 = FormServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.batchGetForms(outbound0);

        var outbound1 = CommentComplianceCaseApprovalRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSanction().getAudit().getCreatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentComplianceCaseApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CommentComplianceCaseApprovalRequest, CommentComplianceCaseApprovalResponse>() {
            @Override
            public CommentComplianceCaseApprovalResponse apply(CommentComplianceCaseApprovalRequest input) {
                return complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentComplianceCaseApproval(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateSanctionApproval", request.getSanction().getSanctionId());
        return DelegateSanctionApprovalResponse.newBuilder()
                .setOutcome(DelegateSanctionApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Recalls a sanction.
     */
    public RecallSanctionResponse recallSanction(RecallSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var builder0 = PreviewAuditEventRequest.newBuilder();
        builder0.setAuditEvent(AuditEvent.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .setSubject(AuditTrailRef.newBuilder()
                        .setDisplayName(request.getSanction().getManager().getDisplayName())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditEvent(outbound0);

        var outbound1 = CommentAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentAttestationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentAttestationApproval(value));
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallSanction", request.getSanction().getSanctionId());
        return RecallSanctionResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(reply0.getAuditEvent().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply1.getAttestation().getBillingAddress().getRegion())
                                .build())
                        .setParentId(request.getSanction().getParentId())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Returns a sanction.
     */
    public GetSanctionApprovalStatusResponse getSanctionApprovalStatus(GetSanctionApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSanctionApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "GetSanctionApprovalStatus");
        return GetSanctionApprovalStatusResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Comments a sanction.
     */
    public CommentSanctionApprovalResponse commentSanctionApproval(CommentSanctionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentSanctionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = mapper.commentSanctionApprovalStage0(request);
        var reply0 = complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitControl(outbound0);

        var outbound1 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getControl().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setPercent(reply0.getControl().getDiscount().getPercent())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.RetentionPolicySyncService");
        var serviceName1 = "acme.audit.v1" + "." + "RetentionPolicySyncService";
        var methodName1 = "Res" + "olveRetentionPolicyConflict";
        var descriptor1 = MethodDescriptor.<ResolveRetentionPolicyConflictRequest, ResolveRetentionPolicyConflictResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveRetentionPolicyConflictRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveRetentionPolicyConflictResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var builder2 = FetchSanctionReportRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setSanctionId(request.getSanctionId());
        var outbound2 = builder2.build();
        var reply2 = complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchSanctionReport(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentSanctionApproval", request.getSanction().getSanctionId());
        return CommentSanctionApprovalResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(reply2.getSanction().getLocation().getRecipient())
                                                .build())
                                        .build())
                                .build())
                        .setDepth(request.getSanction().getDepth())
                        .setManager(UserRef.newBuilder()
                                .setUserId(reply2.getSanction().getManager().getUserId())
                                .build())
                        .setName(reply1.getRetentionPolicy().getName())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }
}
