package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.CheckSanctionEligibilityRequest;
import com.acme.compliance.v1.CheckSanctionEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountControlsRequest;
import com.acme.compliance.v1.CountControlsResponse;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExportControlsRequest;
import com.acme.compliance.v1.ExportControlsResponse;
import com.acme.compliance.v1.FindControlsRequest;
import com.acme.compliance.v1.FindControlsResponse;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.LookupControlRequest;
import com.acme.compliance.v1.LookupControlResponse;
import com.acme.compliance.v1.NormalizeAttestationRequest;
import com.acme.compliance.v1.NormalizeAttestationResponse;
import com.acme.compliance.v1.PeekControlRequest;
import com.acme.compliance.v1.PeekControlResponse;
import com.acme.compliance.v1.ResolveControlsRequest;
import com.acme.compliance.v1.ResolveControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.SuggestControlsRequest;
import com.acme.compliance.v1.SuggestControlsResponse;
import com.acme.compliance.v1.SummarizeControlsRequest;
import com.acme.compliance.v1.SummarizeControlsResponse;
import com.acme.content.v1.Article;
import com.acme.content.v1.ArticleLine;
import com.acme.content.v1.ArticleLineTax;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.CreateArticleRequest;
import com.acme.content.v1.CreateArticleResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.SummarizeLoanApplicationsRequest;
import com.acme.lending.v1.SummarizeLoanApplicationsResponse;
import com.acme.lineage.v1.BatchGetLineageNodesRequest;
import com.acme.lineage.v1.BatchGetLineageNodesResponse;
import com.acme.lineage.v1.LineageNodeServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about controls, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ControlQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlQueryApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceFutureStub complianceControlValidationServiceFuture;

    private final SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final LineageNodeServiceGrpc.LineageNodeServiceBlockingStub lineageLineageNodeServiceBlocking;

    private final ControlQueryRequestMapper mapper;

    private final ComplianceRepository repository;

    public ControlQueryApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ChannelRegistry channels,
            AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceFutureStub complianceControlValidationServiceFuture,
            SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            LineageNodeServiceGrpc.LineageNodeServiceBlockingStub lineageLineageNodeServiceBlocking,
            ControlQueryRequestMapper mapper,
            ComplianceRepository repository) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationValidationServiceBlocking = complianceAttestationValidationServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.complianceControlValidationServiceFuture = complianceControlValidationServiceFuture;
        this.complianceSanctionCalculationServiceBlocking = complianceSanctionCalculationServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.lineageLineageNodeServiceBlocking = lineageLineageNodeServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Returns a control.
     */
    public LookupControlResponse lookupControl(LookupControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "LookupControl");

        var outbound0 = CheckControlEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = complianceControlValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkControlEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = GetControlRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControlId())
                .build();
        var channel1 = channels.lookup("acme.compliance.v1.ControlValidationService");
        var selected1 = ControlValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.getControlRules(outbound1);
        return LookupControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getControl().getAudit().getCreatedBy())
                                .build())
                        .setControlId(request.getControlId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists controls a page at a time.
     */
    public BrowseControlsResponse browseControls(BrowseControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "BrowseControls");

        var outbound0 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound0);
        var reply0 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseControls", request.getPage().getPageToken());
        return BrowseControlsResponse.getDefaultInstance();
    }

    /**
     * Lists controls a page at a time.
     */
    public FindControlsResponse findControls(FindControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "FindControls");

        var builder0 = LintAttestationsRequest.newBuilder();
        builder0.setAttestation(Attestation.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(LintAttestationsRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .setLocale(request.getContext().getLocale())
                .build());
        var outbound0 = builder0.build();
        var reply0 = complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAttestations(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindControls", request.getPage().getPageToken());
        return FindControlsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts controls by status.
     */
    public CountControlsResponse countControls(CountControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "CountControls");

        var outbound0 = BatchGetLineageNodesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<BatchGetLineageNodesRequest, BatchGetLineageNodesResponse> port0 = RpcPort.adapt(lineageLineageNodeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::batchGetLineageNodes);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountControls", request.getPage().getPageToken());
        return CountControlsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several controls at once.
     */
    public ResolveControlsResponse resolveControls(ResolveControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "ResolveControls");

        var outbound0 = CheckSanctionEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSanctionEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSanctionEligibility(value));

        var outbound1 = mapper.resolveControlsStage1(request, reply0);
        var reply1 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintControls(outbound1);
        return ResolveControlsResponse.newBuilder()
                .addControls(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getSanction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching control.
     */
    public ExportControlsResponse exportControls(ExportControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "ExportControls");

        var outbound0 = SummarizeLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeLoanApplicationsRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .setFormat(request.getFormat())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeLoanApplications(value)).orElseThrow();

        var outbound1 = EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setDetails(EstimateControlRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateControl(value));
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportControls", request.getFilter().getQuery());
        return ExportControlsResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getControl().getAudit().getUpdatedAt())
                                .build())
                        .setDescription(reply1.getControl().getDescription())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Summarizes a control.
     */
    public SummarizeControlsResponse summarizeControls(SummarizeControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "SummarizeControls");

        var outbound0 = GetControlRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControlId())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getControlRules(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CreateArticleRequest.newBuilder()
                .setArticle(Article.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getControl().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .addLines(ArticleLine.newBuilder()
                                .setTax(ArticleLineTax.newBuilder()
                                        .setAmount(reply0.getControl().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.content.v1.ArticleService");
        var selected1 = ArticleServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.createArticle(outbound1);

        var outbound2 = BeanCopies.copy(EstimateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(reply1.getArticle().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply1.getArticle().getCounterparty().getDisplayName())
                                                .setUserId(reply0.getControl().getOwner().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateSanctionRequest.class);
        var reply2 = complianceSanctionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateSanction(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeControls", request.getDetails().getFormat());
        return SummarizeControlsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists controls a page at a time.
     */
    public SuggestControlsResponse suggestControls(SuggestControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getContext().getTraceId(), "SuggestControls");

        var outbound0 = NormalizeAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAttestationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<NormalizeAttestationRequest, NormalizeAttestationResponse>() {
            @Override
            public NormalizeAttestationResponse apply(NormalizeAttestationRequest input) {
                return complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAttestation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ExplainAuditEventDecisionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ExplainAuditEventDecisionRequest, ExplainAuditEventDecisionResponse> operation1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainAuditEventDecision;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestControls", request.getPage().getPageToken());
        return SuggestControlsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a control.
     */
    public PeekControlResponse peekControl(PeekControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "PeekControl");

        var outbound0 = CheckComplianceCaseEligibilityRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkComplianceCaseEligibility(outbound0);
        return PeekControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setControlId(request.getControlId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
