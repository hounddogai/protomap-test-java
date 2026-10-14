package com.acme.finance.domains.compliance;

import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.RecallPaymentPromiseRequest;
import com.acme.collections.v1.RecallPaymentPromiseResponse;
import com.acme.collections.v1.StartPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.StartPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.StartWriteOffWorkflowRequest;
import com.acme.collections.v1.StartWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceControlWorkflowRequest;
import com.acme.compliance.v1.AdvanceControlWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlCondition;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.compliance.v1.GetControlApprovalStatusRequest;
import com.acme.compliance.v1.GetControlApprovalStatusResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.RejectControlRequest;
import com.acme.compliance.v1.RejectControlResponse;
import com.acme.compliance.v1.ResumeControlWorkflowRequest;
import com.acme.compliance.v1.ResumeControlWorkflowResponse;
import com.acme.compliance.v1.RetryControlWorkflowRequest;
import com.acme.compliance.v1.RetryControlWorkflowResponse;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.SignalControlWorkflowRequest;
import com.acme.compliance.v1.SignalControlWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.configuration.v1.BrowseConfigSetsRequest;
import com.acme.configuration.v1.BrowseConfigSetsResponse;
import com.acme.configuration.v1.ConfigSetQueryServiceGrpc;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.ExportDocumentsRequest;
import com.acme.documents.v1.ExportDocumentsResponse;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagRouteAction;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a control from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ControlWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlWorkflowApplication.class);

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlLifecycleServiceGrpc.ControlLifecycleServiceBlockingStub complianceControlLifecycleServiceBlocking;

    private final ConfigSetQueryServiceGrpc.ConfigSetQueryServiceBlockingStub configurationConfigSetQueryServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    public ControlWorkflowApplication(
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlLifecycleServiceGrpc.ControlLifecycleServiceBlockingStub complianceControlLifecycleServiceBlocking,
            ConfigSetQueryServiceGrpc.ConfigSetQueryServiceBlockingStub configurationConfigSetQueryServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.collectionsWriteOffWorkflowServiceBlocking = collectionsWriteOffWorkflowServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlLifecycleServiceBlocking = complianceControlLifecycleServiceBlocking;
        this.configurationConfigSetQueryServiceBlocking = configurationConfigSetQueryServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a control.
     */
    public StartControlWorkflowResponse startControlWorkflow(StartControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = GetControlRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControl().getControlId())
                .build();
        var command0 = new DispatchCommand<>("acme.compliance.v1.ControlCalculationService/GetControlRates", outbound0);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getControlRates(command0.payload());

        var outbound1 = BrowseConfigSetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseConfigSetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getControl().getOwner().getUserId())
                        .build())
                .build();
        var reply1 = configurationConfigSetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseConfigSets(outbound1);

        var results2 = new ArrayList<ScheduleControlExpiryResponse>();
        for (var element : request.getControl().getConditionsList()) {
            ScheduleControlExpiryRequest itemRequest = ScheduleControlExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .addConditions(ControlCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build();
            results2.add(complianceControlLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleControlExpiry(itemRequest));
        }
        var reply2 = results2.isEmpty() ? ScheduleControlExpiryResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getControl().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartControlWorkflow", request.getControl().getName());
        return StartControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setVersion(request.getControl().getVersion())
                        .build())
                .setOutcome(StartControlWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a control.
     */
    public AdvanceControlWorkflowResponse advanceControlWorkflow(AdvanceControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = BeanCopies.copy(PauseLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(request.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .build(), PauseLoanWorkflowRequest.class);
        var reply0 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseLoanWorkflow(outbound0);

        var futures1 = request.getControl().getConditionsList().stream()
                .map(element -> RejectControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .addConditions(ControlCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectControl(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? RejectControlResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceControlWorkflow", request.getControl().getControlId());
        return AdvanceControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .setControlId(request.getControlId())
                        .setStatus(request.getControl().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a control.
     */
    public CancelControlWorkflowResponse cancelControlWorkflow(CancelControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = ExportDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getControl().getControlId())
                        .build())
                .build();
        var stream0 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportDocuments(outbound0);
        var reply0 = ExportDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = (BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelControlWorkflow", request.getControl().getName());
        return CancelControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setEffective(request.getControl().getEffective())
                        .build())
                .setOutcome(CancelControlWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a control.
     */
    public RetryControlWorkflowResponse retryControlWorkflow(RetryControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = StartWriteOffWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartWriteOffWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getControl().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsWriteOffWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startWriteOffWorkflow(outbound0), rpcExecutor);

        var outbound1 = RecallPaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPaymentPromise(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryControlWorkflow", request.getControl().getControlId());
        return RetryControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getWriteOff().getAudit().getCreatedAt())
                                .setEtag(reply1.getPaymentPromise().getAudit().getEtag())
                                .build())
                        .setControlId(request.getControlId())
                        .setName(request.getControl().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Resumes a control.
     */
    public ResumeControlWorkflowResponse resumeControlWorkflow(ResumeControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = DelegateAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .addLines(AttestationLine.newBuilder()
                                .setDescription(request.getControl().getDescription())
                                .setTax(AttestationLineTax.newBuilder()
                                        .setAmount(request.getControl().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.AttestationApprovalService");
        var selected0 = AttestationApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.delegateAttestationApproval(outbound0);

        var outbound1 = StartPaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getAttestation().getBillingAddress().getLinesList())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            StartPaymentPromiseWorkflowResponse fetch(StartPaymentPromiseWorkflowRequest input) {
                return collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startPaymentPromiseWorkflow(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentPaymentPromiseApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply1.getPaymentPromise().getOrganizer().getEmail())
                                .build())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<CommentPaymentPromiseApprovalRequest, CommentPaymentPromiseApprovalResponse> port2 = RpcPort.adapt(collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::commentPaymentPromiseApproval);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeControlWorkflow", request.getControl().getControlId());
        return ResumeControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(request.getControl().getOwner())
                        .build())
                .setOutcome(ResumeControlWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a control.
     */
    public SignalControlWorkflowResponse signalControlWorkflow(SignalControlWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalControlWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = LintFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFeatureFlagsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setRoute(FeatureFlagRouteAction.newBuilder()
                                .setTarget(request.getControl().getRoute().getTarget())
                                .setWeight(request.getControl().getRoute().getWeight())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.featureflags.v1.FeatureFlagValidationService");
        var selected0 = FeatureFlagValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.lintFeatureFlags(outbound0);

        var outbound1 = ShareAttestationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAttestationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getFeatureFlag().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ShareAttestationReportResponse fetch(ShareAttestationReportRequest input) {
                return complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAttestationReport(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = GetControlApprovalStatusRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControlId())
                .build();
        var reply2 = complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getControlApprovalStatus(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalControlWorkflow", request.getControl().getControlId());
        return SignalControlWorkflowResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setControlId(request.getControlId())
                        .setOwner(request.getControl().getOwner())
                        .putAllParameters(reply0.getFeatureFlag().getParametersMap())
                        .setPriority(reply1.getAttestation().getCounterparty().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
