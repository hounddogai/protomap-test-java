package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateBankSettlement;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.certificates.v1.EstimateCertificateRequest;
import com.acme.certificates.v1.EstimateCertificateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.customers.v1.WatchCustomersRequest;
import com.acme.customers.v1.WatchCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.insurance.v1.StartClaimWorkflowRequest;
import com.acme.insurance.v1.StartClaimWorkflowResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.CancelRiskScenarioWorkflowRequest;
import com.acme.risk.v1.CancelRiskScenarioWorkflowResponse;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.InspectRiskScenarioWorkflowRequest;
import com.acme.risk.v1.InspectRiskScenarioWorkflowResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.ListRiskScenarioWorkflowsRequest;
import com.acme.risk.v1.ListRiskScenarioWorkflowsResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.PeekRiskScenarioRequest;
import com.acme.risk.v1.PeekRiskScenarioResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.SignalRiskScenarioWorkflowRequest;
import com.acme.risk.v1.SignalRiskScenarioWorkflowResponse;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StartRiskScenarioWorkflowRequest;
import com.acme.risk.v1.StartRiskScenarioWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a risk scenario from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class RiskScenarioWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskScenarioWorkflowApplication.class);

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final RiskRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking;

    private final Executor rpcExecutor;

    public RiskScenarioWorkflowApplication(
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            ApplicationEventPublisher events,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            RiskRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.certificatesCertificateCalculationServiceBlocking = certificatesCertificateCalculationServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordValidationServiceFuture = customersConsentRecordValidationServiceFuture;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.events = events;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.riskStressTestLifecycleServiceBlocking = riskStressTestLifecycleServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    public record SignalRiskScenarioWorkflowStage0Requested(StartRiskRegisterWorkflowRequest request, CompletableFuture<StartRiskRegisterWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a risk scenario.
     */
    public StartRiskScenarioWorkflowResponse startRiskScenarioWorkflow(StartRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = ScheduleStressTestExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleStressTestExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getRequester().getEmail())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRiskScenario().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        RpcPort<ScheduleStressTestExpiryRequest, ScheduleStressTestExpiryResponse> port0 = RpcPort.adapt(riskStressTestLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleStressTestExpiry);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CompareCertificateOptionsRequest.newBuilder()
                .setCertificate(Certificate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                .build())
                        .setBank(CertificateBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply0.getStressTest().getSettlementCase()) {
                                    case BANK -> reply0.getStressTest().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .setBic(switch (reply0.getStressTest().getSettlementCase()) {
                                    case BANK -> reply0.getStressTest().getBank().getBic();
                                    default -> "";
                                })
                                .setIban(switch (reply0.getStressTest().getSettlementCase()) {
                                    case BANK -> reply0.getStressTest().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = certificatesCertificateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCertificateOptions(outbound1);

        var outbound2 = SearchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getCertificate().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(request.getRiskScenario().getCustomer().getCustomerId())
                        .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter2 = new Function<SearchMetricSeriesesRequest, SearchMetricSeriesesResponse>() {
            @Override
            public SearchMetricSeriesesResponse apply(SearchMetricSeriesesRequest input) {
                return metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchMetricSerieses(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartRiskScenarioWorkflow", request.getRiskScenario().getRiskScenarioId());
        return StartRiskScenarioWorkflowResponse.newBuilder()
                .setOutcome(StartRiskScenarioWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setSubject(request.getRiskScenario().getSubject())
                        .build())
                .build();
    }

    /**
     * Advances a risk scenario.
     */
    public AdvanceRiskScenarioWorkflowResponse advanceRiskScenarioWorkflow(AdvanceRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var builder0 = LintRiskRegistersRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setRiskRegister(RiskRegister.newBuilder()
                .addChildren(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(request.getRiskScenario().getAudit().getUpdatedBy())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getRiskScenario().getAssignee().getDisplayName())
                                        .setEmail(request.getRiskScenario().getAssignee().getEmail())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskRegisters(outbound0);

        var outbound1 = EstimateCertificateRequest.newBuilder()
                .setCertificate(Certificate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getRiskRegister().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateCertificateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.certificates.v1.CertificateCalculationService");
        var selected1 = CertificateCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.estimateCertificate(outbound1);

        var outbound2 = JsonCodec.roundTrip(FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build(), FindAuditEventsRequest.class);
        var reply2 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceRiskScenarioWorkflow", request.getRiskScenario().getRiskScenarioId());
        return AdvanceRiskScenarioWorkflowResponse.newBuilder()
                .setOutcome(AdvanceRiskScenarioWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCertificate().getAudit().getEtag())
                                .setUpdatedBy(reply0.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .addComments(RiskScenarioComment.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Cancels a risk scenario.
     */
    public CancelRiskScenarioWorkflowResponse cancelRiskScenarioWorkflow(CancelRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = GenerateRepaymentReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateRepaymentReport(outbound0), rpcExecutor);

        var outbound1 = ResumeClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeClaimWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RpcSupport.unpack(Any.pack(SignalCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply1.getClaim().getRequester().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build()), SignalCoverageWorkflowRequest.class);
        var reply2 = insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalCoverageWorkflow(outbound2);

        var results3 = new ArrayList<StartClaimWorkflowResponse>();
        for (var element : request.getRiskScenario().getCommentsList()) {
            StartClaimWorkflowRequest itemRequest = StartClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results3.add(insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startClaimWorkflow(itemRequest));
        }
        var reply3 = results3.isEmpty() ? StartClaimWorkflowResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelRiskScenarioWorkflow", request.getRiskScenario().getRiskScenarioId());
        return CancelRiskScenarioWorkflowResponse.newBuilder()
                .setOutcome(CancelRiskScenarioWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getClaim().getRequester().getEmail())
                                .build())
                        .setRequester(request.getRiskScenario().getRequester())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Retrys a risk scenario.
     */
    public RetryRiskScenarioWorkflowResponse retryRiskScenarioWorkflow(RetryRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = (RetryRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryRetentionPolicyExportRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getRiskScenario().getRequester().getName())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRiskScenario().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getRiskScenario().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getRiskScenario().getRequester().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryRetentionPolicyExport(outbound0);

        var outbound1 = NormalizeStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeStressTestRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getCustomer().getEmail())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .addLines(StressTestLine.newBuilder()
                                .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.StressTestValidationService");
        var selected1 = StressTestValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.normalizeStressTest(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryRiskScenarioWorkflow", request.getRiskScenario().getRiskScenarioId());
        return RetryRiskScenarioWorkflowResponse.newBuilder()
                .setOutcome(RetryRiskScenarioWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(request.getRiskScenario().getRequester())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Returns a risk scenario.
     */
    public InspectRiskScenarioWorkflowResponse inspectRiskScenarioWorkflow(InspectRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getRiskScenarioId(), "InspectRiskScenarioWorkflow");

        var outbound0 = ResumeRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(request.getContext().getDeadline())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .addChildren(RiskRegister.newBuilder()
                                                .setParentId(request.getRiskScenarioId())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeRiskRegisterWorkflow(value)).orElseThrow();

        var outbound1 = WatchCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchCustomersRequest.Filter.newBuilder()
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getRiskRegister().getManager().getEmail())
                        .build())
                .build();
        var stream1 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCustomers(outbound1);
        var reply1 = WatchCustomersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        return InspectRiskScenarioWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    public ListRiskScenarioWorkflowsResponse listRiskScenarioWorkflows(ListRiskScenarioWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRiskScenarioWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getContext().getTraceId(), "ListRiskScenarioWorkflows");

        var outbound0 = (CommentRiskScenarioApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentRiskScenarioApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRiskScenarioWorkflows", request.getPage().getPageToken());
        return ListRiskScenarioWorkflowsResponse.newBuilder()
                .addRiskScenarios(RiskScenario.newBuilder()
                        .setAssignee(reply0.getRiskScenario().getAssignee())
                        .build())
                .build();
    }

    /**
     * Signals a risk scenario.
     */
    public SignalRiskScenarioWorkflowResponse signalRiskScenarioWorkflow(SignalRiskScenarioWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalRiskScenarioWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = StartRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                        .setUpdatedAt(request.getRiskScenario().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getRiskScenario().getAssignee().getDisplayName())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getRiskScenario().getCustomer().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<StartRiskRegisterWorkflowResponse>();
        events.publishEvent(new SignalRiskScenarioWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ScoreConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getRiskRegister().getLocation().getLocality())
                                .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = customersConsentRecordValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreConsentRecord(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BeanCopies.copy(FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getRiskRegister().getAudit().getCreatedAt())
                        .setOwnerId(request.getRiskScenario().getSla().getPolicyId())
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build(), FindAuditEventsRequest.class);
        var reply2 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(outbound2);

        var outbound3 = BeanCopies.copy(PeekRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenarioId(request.getRiskScenario().getRiskScenarioId())
                .build(), PeekRiskScenarioRequest.class);
        var reply3 = riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekRiskScenario(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalRiskScenarioWorkflow", request.getRiskScenario().getSubject());
        return SignalRiskScenarioWorkflowResponse.newBuilder()
                .setOutcome(SignalRiskScenarioWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply3.getRiskScenario().getRequester().getEmail())
                                .build())
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setSubject(reply3.getRiskScenario().getSubject())
                        .build())
                .build();
    }
}
