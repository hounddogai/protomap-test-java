package com.acme.finance.domains.payroll;

import com.acme.benefits.v1.BenefitPlanServiceGrpc;
import com.acme.benefits.v1.WatchBenefitPlansRequest;
import com.acme.benefits.v1.WatchBenefitPlansResponse;
import com.acme.certificates.v1.BatchGetCertificatesRequest;
import com.acme.certificates.v1.BatchGetCertificatesResponse;
import com.acme.certificates.v1.CertificateServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lineage.v1.CountLineageNodesRequest;
import com.acme.lineage.v1.CountLineageNodesResponse;
import com.acme.lineage.v1.LineageNodeQueryServiceGrpc;
import com.acme.payroll.v1.AdvanceDeductionWorkflowRequest;
import com.acme.payroll.v1.AdvanceDeductionWorkflowResponse;
import com.acme.payroll.v1.ApproveDeductionRequest;
import com.acme.payroll.v1.ApproveDeductionResponse;
import com.acme.payroll.v1.CancelDeductionWorkflowRequest;
import com.acme.payroll.v1.CancelDeductionWorkflowResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionApprovalServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EscalateTaxWithholdingRequest;
import com.acme.payroll.v1.EscalateTaxWithholdingResponse;
import com.acme.payroll.v1.InspectDeductionWorkflowRequest;
import com.acme.payroll.v1.InspectDeductionWorkflowResponse;
import com.acme.payroll.v1.ListDeductionWorkflowsRequest;
import com.acme.payroll.v1.ListDeductionWorkflowsResponse;
import com.acme.payroll.v1.PauseDeductionWorkflowRequest;
import com.acme.payroll.v1.PauseDeductionWorkflowResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecallTaxWithholdingRequest;
import com.acme.payroll.v1.RecallTaxWithholdingResponse;
import com.acme.payroll.v1.ResumeDeductionWorkflowRequest;
import com.acme.payroll.v1.ResumeDeductionWorkflowResponse;
import com.acme.payroll.v1.RetryDeductionWorkflowRequest;
import com.acme.payroll.v1.RetryDeductionWorkflowResponse;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SignalDeductionWorkflowRequest;
import com.acme.payroll.v1.SignalDeductionWorkflowResponse;
import com.acme.payroll.v1.StartDeductionWorkflowRequest;
import com.acme.payroll.v1.StartDeductionWorkflowResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingRouteAction;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a deduction from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DeductionWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionWorkflowApplication.class);

    private final BenefitPlanServiceGrpc.BenefitPlanServiceBlockingStub benefitsBenefitPlanServiceBlocking;

    private final CertificateServiceGrpc.CertificateServiceBlockingStub certificatesCertificateServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final LineageNodeQueryServiceGrpc.LineageNodeQueryServiceBlockingStub lineageLineageNodeQueryServiceBlocking;

    private final DeductionApprovalServiceGrpc.DeductionApprovalServiceBlockingStub payrollDeductionApprovalServiceBlocking;

    private final PayRunWorkflowServiceGrpc.PayRunWorkflowServiceBlockingStub payrollPayRunWorkflowServiceBlocking;

    private final TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceBlockingStub payrollTaxWithholdingApprovalServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final PayrollRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public DeductionWorkflowApplication(
            BenefitPlanServiceGrpc.BenefitPlanServiceBlockingStub benefitsBenefitPlanServiceBlocking,
            CertificateServiceGrpc.CertificateServiceBlockingStub certificatesCertificateServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            LineageNodeQueryServiceGrpc.LineageNodeQueryServiceBlockingStub lineageLineageNodeQueryServiceBlocking,
            DeductionApprovalServiceGrpc.DeductionApprovalServiceBlockingStub payrollDeductionApprovalServiceBlocking,
            PayRunWorkflowServiceGrpc.PayRunWorkflowServiceBlockingStub payrollPayRunWorkflowServiceBlocking,
            TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceBlockingStub payrollTaxWithholdingApprovalServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            PayrollRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.benefitsBenefitPlanServiceBlocking = benefitsBenefitPlanServiceBlocking;
        this.certificatesCertificateServiceBlocking = certificatesCertificateServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.lineageLineageNodeQueryServiceBlocking = lineageLineageNodeQueryServiceBlocking;
        this.payrollDeductionApprovalServiceBlocking = payrollDeductionApprovalServiceBlocking;
        this.payrollPayRunWorkflowServiceBlocking = payrollPayRunWorkflowServiceBlocking;
        this.payrollTaxWithholdingApprovalServiceBlocking = payrollTaxWithholdingApprovalServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    public record CancelDeductionWorkflowStage2Requested(PreviewPayRunRequest request, CompletableFuture<PreviewPayRunResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a deduction.
     */
    public StartDeductionWorkflowResponse startDeductionWorkflow(StartDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = RpcSupport.unpack(Any.pack(ShareTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareTaxWithholdingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build()), ShareTaxWithholdingReportRequest.class);
        var reply0 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareTaxWithholdingReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartDeductionWorkflow", request.getDeduction().getDeductionId());
        return StartDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setVersion(request.getDeduction().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a deduction.
     */
    public AdvanceDeductionWorkflowResponse advanceDeductionWorkflow(AdvanceDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var results0 = new ArrayList<ApproveDeductionResponse>();
        for (var element : request.getDeduction().getConditionsList()) {
            ApproveDeductionRequest itemRequest = ApproveDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .addConditions(DeductionCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build();
            results0.add(payrollDeductionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveDeduction(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ApproveDeductionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BatchGetSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSearchIndexes(value));
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceDeductionWorkflow", request.getDeduction().getDeductionId());
        return AdvanceDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setVersion(request.getDeduction().getVersion())
                        .build())
                .setOutcome(AdvanceDeductionWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels a deduction.
     */
    public CancelDeductionWorkflowResponse cancelDeductionWorkflow(CancelDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = ScheduleTaxWithholdingReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<ScheduleTaxWithholdingReportRequest, ScheduleTaxWithholdingReportResponse>() {
            @Override
            public ScheduleTaxWithholdingReportResponse apply(ScheduleTaxWithholdingReportRequest input) {
                return payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleTaxWithholdingReport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = WatchBenefitPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream1 = benefitsBenefitPlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchBenefitPlans(outbound1);
        var reply1 = WatchBenefitPlansResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = PreviewPayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDeduction().getAudit().getEtag())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(request.getDeduction().getVersion())
                                .build())
                        .addLines(PayRunLine.newBuilder()
                                .setDescription(request.getDeduction().getDescription())
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(request.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<PreviewPayRunResponse>();
        events.publishEvent(new CancelDeductionWorkflowStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = BatchGetCertificatesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var command3 = new DispatchCommand<>("acme.certificates.v1.CertificateService/BatchGetCertificates", outbound3);
        var reply3 = certificatesCertificateServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetCertificates(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelDeductionWorkflow", request.getDeduction().getDeductionId());
        return CancelDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setVersion(request.getDeduction().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a deduction.
     */
    public RetryDeductionWorkflowResponse retryDeductionWorkflow(RetryDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = RecallTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setRoute(TaxWithholdingRouteAction.newBuilder()
                                .setWeight(request.getDeduction().getRoute().getWeight())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.TaxWithholdingApprovalService");
        var selected0 = TaxWithholdingApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.recallTaxWithholding(outbound0);

        var outbound1 = CountLineageNodesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountLineageNodesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getTaxWithholding().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.lineage.v1.LineageNodeQueryService/CountLineageNodes", outbound1);
        var reply1 = lineageLineageNodeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countLineageNodes(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryDeductionWorkflow", request.getDeduction().getDeductionId());
        return RetryDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setDescription(request.getDeduction().getDescription())
                        .build())
                .setOutcome(RetryDeductionWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Pauses a deduction.
     */
    public PauseDeductionWorkflowResponse pauseDeductionWorkflow(PauseDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = RpcSupport.unpack(Any.pack(RetryPayRunWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryPayRunWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()), RetryPayRunWorkflowRequest.class);
        var reply0 = payrollPayRunWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryPayRunWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseDeductionWorkflow", request.getDeduction().getDeductionId());
        return PauseDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .build())
                .setOutcome(PauseDeductionWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Resumes a deduction.
     */
    public ResumeDeductionWorkflowResponse resumeDeductionWorkflow(ResumeDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var builder0 = CheckPostingBatchEligibilityRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getDeduction().getOwner().getEmail())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build());
        builder0.setPostingBatch(PostingBatch.newBuilder()
                .addLines(PostingBatchLine.newBuilder()
                        .setTax(PostingBatchLineTax.newBuilder()
                                .setAmount(request.getDeduction().getDiscount().getAmount())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPostingBatchEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeDeductionWorkflow", request.getDeduction().getDeductionId());
        return ResumeDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setVersion(request.getDeduction().getVersion())
                        .build())
                .setOutcome(ResumeDeductionWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public InspectDeductionWorkflowResponse inspectDeductionWorkflow(InspectDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "InspectDeductionWorkflow");
        return InspectDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .build();
    }

    /**
     * Lists deductions a page at a time.
     */
    public ListDeductionWorkflowsResponse listDeductionWorkflows(ListDeductionWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDeductionWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getContext().getTraceId(), "ListDeductionWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDeductionWorkflows", request.getPage().getPageToken());
        return ListDeductionWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a deduction.
     */
    public SignalDeductionWorkflowResponse signalDeductionWorkflow(SignalDeductionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalDeductionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = BeanCopies.copy(ShareTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareTaxWithholdingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build(), ShareTaxWithholdingReportRequest.class);
        var reply0 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareTaxWithholdingReport(outbound0);

        var outbound1 = EscalateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .setRoute(TaxWithholdingRouteAction.newBuilder()
                                .setTarget(reply0.getTaxWithholding().getRoute().getTarget())
                                .build())
                        .build())
                .build();
        Function<EscalateTaxWithholdingRequest, EscalateTaxWithholdingResponse> operation1 = payrollTaxWithholdingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::escalateTaxWithholding;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalDeductionWorkflow", request.getDeduction().getDeductionId());
        return SignalDeductionWorkflowResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setDescription(reply0.getTaxWithholding().getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .putAllParameters(request.getDeduction().getParametersMap())
                        .setVersion(reply1.getTaxWithholding().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
