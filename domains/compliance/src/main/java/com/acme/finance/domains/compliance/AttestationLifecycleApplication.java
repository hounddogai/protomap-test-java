package com.acme.finance.domains.compliance;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.collections.v1.SignalWriteOffWorkflowRequest;
import com.acme.collections.v1.SignalWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ActivateAttestationRequest;
import com.acme.compliance.v1.ActivateAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationCardSettlement;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExpireAttestationRequest;
import com.acme.compliance.v1.ExpireAttestationResponse;
import com.acme.compliance.v1.FetchAttestationReportRequest;
import com.acme.compliance.v1.FetchAttestationReportResponse;
import com.acme.compliance.v1.GetAttestationLifecycleRequest;
import com.acme.compliance.v1.GetAttestationLifecycleResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.RenewAttestationRequest;
import com.acme.compliance.v1.RenewAttestationResponse;
import com.acme.compliance.v1.SuspendAttestationRequest;
import com.acme.compliance.v1.SuspendAttestationResponse;
import com.acme.compliance.v1.TerminateAttestationRequest;
import com.acme.compliance.v1.TerminateAttestationResponse;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.performers.v1.BatchGetPerformersRequest;
import com.acme.performers.v1.BatchGetPerformersResponse;
import com.acme.performers.v1.PerformerServiceGrpc;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves an attestation between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class AttestationLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationLifecycleApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final AttestationLifecycleRequestMapper mapper;

    private final PerformerServiceGrpc.PerformerServiceBlockingStub performersPerformerServiceBlocking;

    private final ComplianceRepository repository;

    private final ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    public AttestationLifecycleApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            ChannelRegistry channels,
            WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            AttestationLifecycleRequestMapper mapper,
            PerformerServiceGrpc.PerformerServiceBlockingStub performersPerformerServiceBlocking,
            ComplianceRepository repository,
            ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffWorkflowServiceBlocking = collectionsWriteOffWorkflowServiceBlocking;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceAttestationValidationServiceBlocking = complianceAttestationValidationServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.performersPerformerServiceBlocking = performersPerformerServiceBlocking;
        this.repository = repository;
        this.riskExposureApprovalServiceBlocking = riskExposureApprovalServiceBlocking;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
    }

    /**
     * Activates an attestation.
     */
    public ActivateAttestationResponse activateAttestation(ActivateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "ActivateAttestation");

        var outbound0 = mapper.activateAttestationStage0(request);
        var reply0 = bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelBankAccountWorkflow(outbound0);

        var builder1 = SubmitExposureRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(SubmitExposureRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(reply0.getBankAccount().getHolder().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = riskExposureApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitExposure(outbound1);

        var outbound2 = SignalWriteOffWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalWriteOffWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getBankAccount().getContact().getEmail())
                                .setName(reply1.getExposure().getContact().getName())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setToken(reply1.getExposure().getCard().getToken())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsWriteOffWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalWriteOffWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getAttestationId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input3 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateAttestation", request.getDetails().getReasonCode());
        return ActivateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setOutcome(ActivateAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends an attestation.
     */
    public SuspendAttestationResponse suspendAttestation(SuspendAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "SuspendAttestation");

        var outbound0 = (RecalculateControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateControls(outbound0);

        var outbound1 = BeanCopies.copy(FetchAttestationReportRequest.newBuilder()
                .setAttestationId(request.getAttestationId())
                .setContext(request.getContext())
                .build(), FetchAttestationReportRequest.class);
        var reply1 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchAttestationReport(outbound1);

        var builder2 = NormalizeComplianceCaseRequest.newBuilder();
        builder2.setComplianceCase(ComplianceCase.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                        .build())
                .build());
        builder2.setContext(request.getContext());
        var outbound2 = builder2.build();
        var reply2 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeComplianceCase(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendAttestation", request.getDetails().getReasonCode());
        return SuspendAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setOutcome(SuspendAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates an attestation.
     */
    public TerminateAttestationResponse terminateAttestation(TerminateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "TerminateAttestation");

        var outbound0 = RpcSupport.unpack(Any.pack(ApproveLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build()), ApproveLoanRequest.class);
        var reply0 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveLoan(outbound0);

        var outbound1 = BeanCopies.copy(BatchGetPerformersRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetPerformersRequest.class);
        var reply1 = performersPerformerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetPerformers(outbound1);

        var outbound2 = EstimateAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setCard(AttestationCardSettlement.newBuilder()
                                .setLast4(reply0.getLoan().getCard().getLast4())
                                .setNetwork(reply0.getLoan().getCard().getNetwork())
                                .build())
                        .setNumber(reply0.getLoan().getHolder().getNumber())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAttestation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateAttestation", request.getDetails().getReasonCode());
        return TerminateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getAttestation().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setOutcome(TerminateAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews an attestation.
     */
    public RenewAttestationResponse renewAttestation(RenewAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "RenewAttestation");

        var outbound0 = StartRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow", outbound0);
        var reply0 = riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startRiskRegisterWorkflow(command0.payload());

        var builder1 = LintAttestationsRequest.newBuilder();
        builder1.setAttestation(Attestation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                        .build())
                .build());
        builder1.setContext(request.getContext());
        builder1.setDetails(LintAttestationsRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(reply0.getRiskRegister().getManager().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAttestations(outbound1);

        var outbound2 = ApproveClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.ClaimApprovalService");
        var selected2 = ClaimApprovalServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.approveClaim(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewAttestation", request.getDetails().getReasonCode());
        return RenewAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getClaim().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getAttestation().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setComplianceCaseId(reply1.getAttestation().getCounterparty().getComplianceCaseId())
                                .setPriority(reply2.getClaim().getPriority())
                                .build())
                        .build())
                .setOutcome(RenewAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Expires an attestation.
     */
    public ExpireAttestationResponse expireAttestation(ExpireAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "ExpireAttestation");

        var outbound0 = BeanCopies.copy(ValidateAttestationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateAttestationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build(), ValidateAttestationRequest.class);
        var reply0 = complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAttestation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireAttestation", request.getDetails().getReasonCode());
        return ExpireAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .setOutcome(ExpireAttestationResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public GetAttestationLifecycleResponse getAttestationLifecycle(GetAttestationLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAttestationLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "GetAttestationLifecycle");
        return GetAttestationLifecycleResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .build();
    }
}
