package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveControlRequest;
import com.acme.compliance.v1.ApproveControlResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.CommentControlApprovalRequest;
import com.acme.compliance.v1.CommentControlApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlCondition;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.EscalateControlRequest;
import com.acme.compliance.v1.EscalateControlResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetControlApprovalStatusRequest;
import com.acme.compliance.v1.GetControlApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingComplianceCasesRequest;
import com.acme.compliance.v1.ListPendingComplianceCasesResponse;
import com.acme.compliance.v1.ListPendingControlsRequest;
import com.acme.compliance.v1.ListPendingControlsResponse;
import com.acme.compliance.v1.PeekControlRequest;
import com.acme.compliance.v1.PeekControlResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.RecallControlRequest;
import com.acme.compliance.v1.RecallControlResponse;
import com.acme.compliance.v1.RejectComplianceCaseRequest;
import com.acme.compliance.v1.RejectComplianceCaseResponse;
import com.acme.compliance.v1.RejectControlRequest;
import com.acme.compliance.v1.RejectControlResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SearchComplianceCasesRequest;
import com.acme.compliance.v1.SearchComplianceCasesResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.datasets.v1.Dataset;
import com.acme.datasets.v1.DatasetServiceGrpc;
import com.acme.datasets.v1.UpdateDatasetRequest;
import com.acme.datasets.v1.UpdateDatasetResponse;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.forms.v1.BrowseFormsRequest;
import com.acme.forms.v1.BrowseFormsResponse;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.risk.v1.DelegateRiskRegisterApprovalRequest;
import com.acme.risk.v1.DelegateRiskRegisterApprovalResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes controls through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ControlApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlApprovalApplication.class);

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceFutureStub complianceComplianceCaseApprovalServiceFuture;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final DatasetServiceGrpc.DatasetServiceBlockingStub datasetsDatasetServiceBlocking;

    private final ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking;

    private final ComplianceRepository repository;

    public ControlApprovalApplication(
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceFutureStub complianceComplianceCaseApprovalServiceFuture,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            DatasetServiceGrpc.DatasetServiceBlockingStub datasetsDatasetServiceBlocking,
            ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking,
            ComplianceRepository repository) {
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceComplianceCaseApprovalServiceBlocking = complianceComplianceCaseApprovalServiceBlocking;
        this.complianceComplianceCaseApprovalServiceFuture = complianceComplianceCaseApprovalServiceFuture;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceComplianceCaseServiceBlocking = complianceComplianceCaseServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlQueryServiceBlocking = complianceControlQueryServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.datasetsDatasetServiceBlocking = datasetsDatasetServiceBlocking;
        this.expensesExpenseReportServiceAsync = expensesExpenseReportServiceAsync;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.formsFormQueryServiceBlocking = formsFormQueryServiceBlocking;
        this.repository = repository;
    }

    /**
     * Submits a control.
     */
    public SubmitControlResponse submitControl(SubmitControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = RejectComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getControl().getOwner().getUserId())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setDisplayName(request.getControl().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectComplianceCase(outbound0);

        var outbound1 = BrowseFormsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFormsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getComplianceCase().getSla().getDueAt())
                        .putAllLabels(request.getControl().getParametersMap())
                        .setOwnerId(request.getControl().getOwner().getUserId())
                        .build())
                .build();
        var reply1 = formsFormQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseForms(outbound1);

        var outbound2 = CreateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setBlock(FeatureFlagBlockAction.newBuilder()
                                .setNotifyEmail(request.getControl().getBlock().getNotifyEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFeatureFlag(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = BatchCreateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateExpenseReportsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateExpenseReportsResponse.getDefaultInstance());
        var input3 = expensesExpenseReportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateExpenseReports(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setEtag(request.getControl().getAudit().getEtag())
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitControl", request.getControl().getControlId());
        return SubmitControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getComplianceCase().getCustomer().getDisplayName())
                                .setUserId(reply2.getFeatureFlag().getOwner().getUserId())
                                .build())
                        .putAllParameters(request.getControl().getParametersMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves a control.
     */
    public ApproveControlResponse approveControl(ApproveControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = EscalateComplianceCaseRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateComplianceCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getControl().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = complianceComplianceCaseApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).escalateComplianceCase(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PeekControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControl().getControlId())
                .build();
        var reply1 = complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekControl(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveControl", request.getControlId());
        return ApproveControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(reply1.getControl().getControlId())
                        .setName(request.getControl().getName())
                        .putAllParameters(request.getControl().getParametersMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a control.
     */
    public RejectControlResponse rejectControl(RejectControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = JsonCodec.roundTrip(BrowseAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getUpdatedAt())
                        .build())
                .build(), BrowseAuditEventsRequest.class);
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAuditEvents(outbound0);

        var outbound1 = StreamSanctionMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamSanctionMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .setSince(request.getControl().getAudit().getUpdatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getControl().getOwner().getEmail())
                        .build())
                .build();
        var stream1 = complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamSanctionMetrics(outbound1);
        var reply1 = StreamSanctionMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectControl", request.getControl().getControlId());
        return RejectControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(request.getControl().getOwner())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a control.
     */
    public EscalateControlResponse escalateControl(EscalateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var results0 = new ArrayList<SimulateControlResponse>();
        for (var element : request.getControl().getConditionsList()) {
            SimulateControlRequest itemRequest = SimulateControlRequest.newBuilder()
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
            results0.add(complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateControl(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SimulateControlResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPendingComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingComplianceCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getControl().getParametersMap())
                        .setOwnerId(reply0.getControl().getOwner().getUserId())
                        .build())
                .build();
        RpcPort<ListPendingComplianceCasesRequest, ListPendingComplianceCasesResponse> port1 = RpcPort.adapt(complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listPendingComplianceCases);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateControl", request.getControl().getControlId());
        return EscalateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setVersion(request.getControl().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists controls a page at a time.
     */
    public ListPendingControlsResponse listPendingControls(ListPendingControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "ListPendingControls");

        var outbound0 = RpcSupport.unpack(Any.pack(BrowseControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseControlsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .build()), BrowseControlsRequest.class);
        var reply0 = complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseControls(outbound0);

        var builder1 = RecalculateAttestationsRequest.newBuilder();
        builder1.setAttestation(Attestation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getFilter().getCreatedAfter())
                        .build())
                .addLines(AttestationLine.newBuilder()
                        .putAllAttributes(reply0.getResult().getDetailsMap())
                        .build())
                .build());
        builder1.setContext(request.getContext());
        builder1.setDetails(RecalculateAttestationsRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAttestations(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingControls", request.getPage().getPageToken());
        return ListPendingControlsResponse.newBuilder()
                .addControls(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }

    /**
     * Recalls a control.
     */
    public RecallControlResponse recallControl(RecallControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getControl().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.LedgerAccountService");
        var serviceName0 = "acme.ledger.v1" + "." + "LedgerAccountService";
        var methodName0 = "Clo" + "neLedgerAccount";
        var descriptor0 = MethodDescriptor.<CloneLedgerAccountRequest, CloneLedgerAccountResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CloneLedgerAccountRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CloneLedgerAccountResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = DelegateRiskRegisterApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getLedgerAccount().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.RiskRegisterApprovalService");
        var selected1 = RiskRegisterApprovalServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.delegateRiskRegisterApproval(outbound1);

        var outbound2 = BeanCopies.copy(RejectComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRiskRegister().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RejectComplianceCaseRequest.class);
        var reply2 = complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectComplianceCase(outbound2);

        var outbound3 = (ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getRiskRegister().getManager().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply3 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallControl", request.getControl().getControlId());
        return RecallControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply3.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .setPriority(reply3.getComplianceCase().getPriority())
                        .setStatus(request.getControl().getStatus())
                        .build())
                .setOutcome(RecallControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a control.
     */
    public GetControlApprovalStatusResponse getControlApprovalStatus(GetControlApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetControlApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "GetControlApprovalStatus");
        return GetControlApprovalStatusResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .build())
                .build();
    }

    /**
     * Comments a control.
     */
    public CommentControlApprovalResponse commentControlApproval(CommentControlApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentControlApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = RpcSupport.unpack(Any.pack(UpdateDatasetRequest.newBuilder()
                .setContext(request.getContext())
                .setDataset(Dataset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build()), UpdateDatasetRequest.class);
        var reply0 = datasetsDatasetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateDataset(outbound0);

        var outbound1 = SearchComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchComplianceCasesRequest.Filter.newBuilder()
                        .setRegion(switch (reply0.getDataset().getLocatorCase()) {
                            case ADDRESS -> reply0.getDataset().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = complianceComplianceCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchComplianceCases(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentControlApproval", request.getControl().getControlId());
        return CommentControlApprovalResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setName(request.getControl().getName())
                        .setOwner(reply0.getDataset().getOwner())
                        .build())
                .setOutcome(CommentControlApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
