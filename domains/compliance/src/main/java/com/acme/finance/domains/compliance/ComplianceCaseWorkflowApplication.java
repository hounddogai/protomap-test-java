package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ScheduleRetentionPolicyExportRequest;
import com.acme.audit.v1.ScheduleRetentionPolicyExportResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsRequest;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.StartComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.StartComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.forms.v1.ExportFormsRequest;
import com.acme.forms.v1.ExportFormsResponse;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.notifications.v1.NormalizeNotificationRequest;
import com.acme.notifications.v1.NormalizeNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a compliance case from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ComplianceCaseWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseWorkflowApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final ComplianceCaseWorkflowRequestMapper mapper;

    private final ComplianceRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    public ComplianceCaseWorkflowApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            ComplianceCaseWorkflowRequestMapper mapper,
            ComplianceRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceComplianceCaseApprovalServiceBlocking = complianceComplianceCaseApprovalServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.formsFormQueryServiceBlocking = formsFormQueryServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
    }

    /**
     * Starts a compliance case.
     */
    public StartComplianceCaseWorkflowResponse startComplianceCaseWorkflow(StartComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = BeanCopies.copy(ExplainAccessPolicyDecisionRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAccessPolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build(), ExplainAccessPolicyDecisionRequest.class);
        var reply0 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAccessPolicyDecision(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return StartComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setStatus(request.getComplianceCase().getStatus())
                        .build())
                .setOutcome(StartComplianceCaseWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Advances a compliance case.
     */
    public AdvanceComplianceCaseWorkflowResponse advanceComplianceCaseWorkflow(AdvanceComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var builder0 = CompareAttestationOptionsRequest.newBuilder();
        builder0.setAttestation(Attestation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                        .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                        .build())
                .setCounterparty(ComplianceCaseRef.newBuilder()
                        .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                        .build())
                .setDueAt(request.getComplianceCase().getSla().getDueAt())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAttestationOptions(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ResumeExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()), ResumeExposureWorkflowRequest.class);
        var reply1 = riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeExposureWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return AdvanceComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getExposure().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .putAllCustomFields(request.getComplianceCase().getCustomFieldsMap())
                        .build())
                .build();
    }

    /**
     * Cancels a compliance case.
     */
    public CancelComplianceCaseWorkflowResponse cancelComplianceCaseWorkflow(CancelComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = RecallComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecallComplianceCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RecallComplianceCaseResponse fetch(RecallComplianceCaseRequest input) {
                return complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallComplianceCase(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return CancelComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setPriority(reply0.getComplianceCase().getPriority())
                        .setStatus(request.getComplianceCase().getStatus())
                        .build())
                .setOutcome(CancelComplianceCaseWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Retrys a compliance case.
     */
    public RetryComplianceCaseWorkflowResponse retryComplianceCaseWorkflow(RetryComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var results0 = new ArrayList<ExportFormsResponse>();
        for (var element : request.getComplianceCase().getCommentsList()) {
            ExportFormsRequest itemRequest = ExportFormsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportFormsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .putAllLabels(request.getComplianceCase().getCustomFieldsMap())
                        .setOwnerId(element.getCommentId())
                        .build())
                .build();
            var responses = formsFormQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportForms(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? ExportFormsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return RetryComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .build();
    }

    /**
     * Pauses a compliance case.
     */
    public PauseComplianceCaseWorkflowResponse pauseComplianceCaseWorkflow(PauseComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ListPendingPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getSla().getDueAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getComplianceCase().getAssignee().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPolicies(outbound0));

        var outbound1 = CompareComplianceCaseOptionsRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setChannel(ComplianceCaseChannelOrigin.newBuilder()
                                .setChannel(switch (request.getComplianceCase().getOriginCase()) {
                                    case CHANNEL -> request.getComplianceCase().getChannel().getChannel();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareComplianceCaseOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return PauseComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setPriority(request.getComplianceCase().getPriority())
                        .build())
                .setOutcome(PauseComplianceCaseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a compliance case.
     */
    public ResumeComplianceCaseWorkflowResponse resumeComplianceCaseWorkflow(ResumeComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var results0 = new ArrayList<SubmitControlResponse>();
        for (var element : request.getComplianceCase().getCommentsList()) {
            SubmitControlRequest itemRequest = SubmitControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setDetails(SubmitControlRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .setComment(element.getCommentId())
                        .build())
                .build();
            results0.add(complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitControl(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SubmitControlResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ShareAttestationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAttestationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAttestationReport(outbound1);

        var outbound2 = NormalizeNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeNotificationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getAttestation().getCounterparty().getDisplayName())
                                .setUserId(reply0.getControl().getOwner().getUserId())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setNumber(request.getComplianceCase().getCustomer().getNumber())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.notifications.v1.NotificationValidationService");
        var selected2 = NotificationValidationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.normalizeNotification(outbound2);

        var outbound3 = ScheduleRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(reply0.getControl().getBlock().getReason())
                .build();
        var command3 = new DispatchCommand<>("acme.audit.v1.RetentionPolicyExportService/ScheduleRetentionPolicyExport", outbound3);
        var reply3 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRetentionPolicyExport(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeComplianceCaseWorkflow", request.getComplianceCase().getComplianceCaseId());
        return ResumeComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getAttestation().getAudit().getEtag())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .addAllTags(reply2.getNotification().getTagsList())
                        .build())
                .setOutcome(ResumeComplianceCaseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public InspectComplianceCaseWorkflowResponse inspectComplianceCaseWorkflow(InspectComplianceCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectComplianceCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "InspectComplianceCaseWorkflow");

        var outbound0 = ListSanctionReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSanctionReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getComplianceCaseId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.SanctionReportingService");
        var serviceName0 = "acme.compliance.v1" + "." + "SanctionReportingService";
        var methodName0 = "Lis" + "tSanctionReports";
        var descriptor0 = MethodDescriptor.<ListSanctionReportsRequest, ListSanctionReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ListSanctionReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListSanctionReportsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ListAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var adapter1 = new Function<ListAuditEventsRequest, ListAuditEventsResponse>() {
            @Override
            public ListAuditEventsResponse apply(ListAuditEventsRequest input) {
                return auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        return InspectComplianceCaseWorkflowResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public ListComplianceCaseWorkflowsResponse listComplianceCaseWorkflows(ListComplianceCaseWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListComplianceCaseWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "ListComplianceCaseWorkflows");

        var outbound0 = mapper.listComplianceCaseWorkflowsStage0(request);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteControl(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListComplianceCaseWorkflows", request.getPage().getPageToken());
        return ListComplianceCaseWorkflowsResponse.newBuilder()
                .addComplianceCases(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getControl().getOwner().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
