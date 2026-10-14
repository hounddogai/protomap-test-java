package com.acme.finance.domains.payroll;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.PreviewUserRequest;
import com.acme.identity.v1.PreviewUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.SyncUnderwritingsRequest;
import com.acme.insurance.v1.SyncUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.payroll.v1.AdvancePayRunWorkflowRequest;
import com.acme.payroll.v1.AdvancePayRunWorkflowResponse;
import com.acme.payroll.v1.CancelPayRunWorkflowRequest;
import com.acme.payroll.v1.CancelPayRunWorkflowResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DelegatePayRunApprovalRequest;
import com.acme.payroll.v1.DelegatePayRunApprovalResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.InspectPayRunWorkflowRequest;
import com.acme.payroll.v1.InspectPayRunWorkflowResponse;
import com.acme.payroll.v1.ListPayRunWorkflowsRequest;
import com.acme.payroll.v1.ListPayRunWorkflowsResponse;
import com.acme.payroll.v1.PausePayRunWorkflowRequest;
import com.acme.payroll.v1.PausePayRunWorkflowResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubLifecycleServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallPayStubRequest;
import com.acme.payroll.v1.RecallPayStubResponse;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.ResolvePayStubsRequest;
import com.acme.payroll.v1.ResolvePayStubsResponse;
import com.acme.payroll.v1.ResumePayRunWorkflowRequest;
import com.acme.payroll.v1.ResumePayRunWorkflowResponse;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.payroll.v1.SchedulePayStubExpiryRequest;
import com.acme.payroll.v1.SchedulePayStubExpiryResponse;
import com.acme.payroll.v1.SignalPayRunWorkflowRequest;
import com.acme.payroll.v1.SignalPayRunWorkflowResponse;
import com.acme.payroll.v1.StartPayRunWorkflowRequest;
import com.acme.payroll.v1.StartPayRunWorkflowResponse;
import com.acme.payroll.v1.SubmitPayRunRequest;
import com.acme.payroll.v1.SubmitPayRunResponse;
import com.acme.returns.v1.CloneReturnRequestRequest;
import com.acme.returns.v1.CloneReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestBankSettlement;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a pay run from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayRunWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunWorkflowApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking;

    private final PayRunApprovalServiceGrpc.PayRunApprovalServiceBlockingStub payrollPayRunApprovalServiceBlocking;

    private final PayStubApprovalServiceGrpc.PayStubApprovalServiceBlockingStub payrollPayStubApprovalServiceBlocking;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final PayrollRepository repository;

    private final ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public PayRunWorkflowApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            ChannelRegistry channels,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking,
            PayRunApprovalServiceGrpc.PayRunApprovalServiceBlockingStub payrollPayRunApprovalServiceBlocking,
            PayStubApprovalServiceGrpc.PayStubApprovalServiceBlockingStub payrollPayStubApprovalServiceBlocking,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            PayrollRepository repository,
            ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.channels = channels;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollDeductionReportingServiceBlocking = payrollDeductionReportingServiceBlocking;
        this.payrollPayRunApprovalServiceBlocking = payrollPayRunApprovalServiceBlocking;
        this.payrollPayStubApprovalServiceBlocking = payrollPayStubApprovalServiceBlocking;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.repository = repository;
        this.returnsReturnRequestServiceBlocking = returnsReturnRequestServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Starts a pay run.
     */
    public StartPayRunWorkflowResponse startPayRunWorkflow(StartPayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var results0 = new ArrayList<SubmitPayRunResponse>();
        for (var element : request.getPayRun().getLinesList()) {
            SubmitPayRunRequest itemRequest = SubmitPayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(payrollPayRunApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitPayRun(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SubmitPayRunResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getPayRun().getCounterparty().getDeductionId())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyVersions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPayRunWorkflow", request.getPayRun().getPayRunId());
        return StartPayRunWorkflowResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .putAllMetadata(request.getPayRun().getMetadataMap())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a pay run.
     */
    public AdvancePayRunWorkflowResponse advancePayRunWorkflow(AdvancePayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = CommentRiskScenarioApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentRiskScenarioApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentRiskScenarioApproval(value));

        var outbound1 = ValidateUnderwritingImportRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(switch (reply0.getRiskScenario().getOriginCase()) {
                                    case CUSTOMER -> reply0.getRiskScenario().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .setSla(UnderwritingSla.newBuilder()
                                .setPolicyId(reply0.getRiskScenario().getSla().getPolicyId())
                                .build())
                        .build())
                .build();
        var reply1 = insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwritingImport(outbound1);

        var outbound2 = UpdateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPayRun().getBillingAddress().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getRiskScenario().getOriginCase()) {
                                    case CUSTOMER -> reply0.getRiskScenario().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateVenue(outbound2);

        var outbound3 = ReinstateDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                .setDetails(ReinstateDeductionRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .setUserId(reply1.getUnderwriting().getAssignee().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(reply1.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply3 = payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateDeduction(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePayRunWorkflow", request.getPayRun().getPayRunId());
        return AdvancePayRunWorkflowResponse.newBuilder()
                .setOutcome(AdvancePayRunWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getVenue().getAddress().getLinesList())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(reply0.getRiskScenario().getSla().getDueAt())
                        .setPayRunId(request.getPayRunId())
                        .setStatus(request.getPayRun().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a pay run.
     */
    public CancelPayRunWorkflowResponse cancelPayRunWorkflow(CancelPayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getPayRun().getCounterparty().getDeductionId())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound0);
        var reply0 = ExportUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPayRunWorkflow", request.getPayRun().getPayRunId());
        return CancelPayRunWorkflowResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Retrys a pay run.
     */
    public RetryPayRunWorkflowResponse retryPayRunWorkflow(RetryPayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryPayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = SchedulePayStubExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayRun().getAudit().getEtag())
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PayStubCardSettlement.newBuilder()
                                .setToken(request.getPayRun().getCard().getToken())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubLifecycleService");
        var selected0 = PayStubLifecycleServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.schedulePayStubExpiry(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryPayRunWorkflow", request.getPayRun().getPayRunId());
        return RetryPayRunWorkflowResponse.newBuilder()
                .setOutcome(RetryPayRunWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a pay run.
     */
    public PausePayRunWorkflowResponse pausePayRunWorkflow(PausePayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PausePayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = SyncUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncUnderwritings(outbound0);

        var outbound1 = PreviewUserRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewUserRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .setUser(User.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPayRun().getBillingAddress().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewUser(value));

        var outbound2 = NotifyFiscalPeriodWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyFiscalPeriodWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .setName(reply0.getUnderwriting().getRequester().getName())
                                .setPhone(reply1.getUser().getContact().getPhone())
                                .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyFiscalPeriodWatchers(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = JsonCodec.roundTrip(ResolvePayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolvePayStubsRequest.class);
        var reply3 = payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePayStubs(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PausePayRunWorkflow", request.getPayRun().getPayRunId());
        return PausePayRunWorkflowResponse.newBuilder()
                .setOutcome(PausePayRunWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getUser().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getFiscalPeriod().getLocation().getLocality())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(reply0.getUnderwriting().getSla().getDueAt())
                        .putAllMetadata(request.getPayRun().getMetadataMap())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Resumes a pay run.
     */
    public ResumePayRunWorkflowResponse resumePayRunWorkflow(ResumePayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumePayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = DelegatePayRunApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .setBank(PayRunBankSettlement.newBuilder()
                                .setBic(request.getPayRun().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPayRun().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<DelegatePayRunApprovalRequest, DelegatePayRunApprovalResponse>() {
            @Override
            public DelegatePayRunApprovalResponse apply(DelegatePayRunApprovalRequest input) {
                return payrollPayRunApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegatePayRunApproval(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        List<RecallPayStubResponse> results1 = request.getPayRun().getAttachmentsList().stream()
                .map(element -> RecallPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addAttachments(PayStubAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayStubApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPayStub(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? RecallPayStubResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumePayRunWorkflow", request.getPayRun().getPayRunId());
        return ResumePayRunWorkflowResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(reply0.getPayRun().getCounterparty().getDeductionId())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .setStatus(request.getPayRun().getStatus())
                        .setTotal(reply0.getPayRun().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public InspectPayRunWorkflowResponse inspectPayRunWorkflow(InspectPayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "InspectPayRunWorkflow");
        return InspectPayRunWorkflowResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public ListPayRunWorkflowsResponse listPayRunWorkflows(ListPayRunWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPayRunWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "ListPayRunWorkflows");

        var outbound0 = CommentPayStubApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubApprovalService");
        var selected0 = PayStubApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.commentPayStubApproval(outbound0);

        var outbound1 = CloneReturnRequestRequest.newBuilder()
                .setContext(request.getContext())
                .setReturnRequest(ReturnRequest.newBuilder()
                        .setBank(ReturnRequestBankSettlement.newBuilder()
                                .setIban(switch (reply0.getPayStub().getSettlementCase()) {
                                    case BANK -> reply0.getPayStub().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getPayStub().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = returnsReturnRequestServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneReturnRequest(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPayRunWorkflows", request.getPage().getPageToken());
        return ListPayRunWorkflowsResponse.newBuilder()
                .addPayRuns(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getReturnRequest().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals a pay run.
     */
    public SignalPayRunWorkflowResponse signalPayRunWorkflow(SignalPayRunWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPayRunWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound0), rpcExecutor);

        var outbound1 = GenerateDeductionReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollDeductionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDeductionReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPayRunWorkflow", request.getPayRun().getPayRunId());
        return SignalPayRunWorkflowResponse.newBuilder()
                .setOutcome(SignalPayRunWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getNotification().getAudit().getUpdatedAt())
                                .build())
                        .setNumber(request.getPayRun().getNumber())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
