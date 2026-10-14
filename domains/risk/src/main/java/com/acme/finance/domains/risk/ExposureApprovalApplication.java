package com.acme.finance.domains.risk;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.risk.v1.ApproveExposureRequest;
import com.acme.risk.v1.ApproveExposureResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CommentExposureApprovalRequest;
import com.acme.risk.v1.CommentExposureApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureLimit;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetExposureApprovalStatusRequest;
import com.acme.risk.v1.GetExposureApprovalStatusResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.RecallExposureRequest;
import com.acme.risk.v1.RecallExposureResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectExposureRequest;
import com.acme.risk.v1.RejectExposureResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes exposures through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ExposureApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ExposureApprovalApplication.class);

    private final ChannelRegistry channels;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final ExposureApprovalRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final RiskRepository repository;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final Executor rpcExecutor;

    public ExposureApprovalApplication(
            ChannelRegistry channels,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            ApplicationEventPublisher events,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            ExposureApprovalRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            RiskRepository repository,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.events = events;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    public record DelegateExposureApprovalStage2Requested(LintRiskRegistersRequest request, CompletableFuture<LintRiskRegistersResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an exposure.
     */
    public SubmitExposureResponse submitExposure(SubmitExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RetryLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryLoanWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .build())
                        .setBank(LoanBankFunding.newBuilder()
                                .setBic(switch (request.getExposure().getFundingCase()) {
                                    case BANK -> request.getExposure().getBank().getBic();
                                    default -> "";
                                })
                                .setIban(switch (request.getExposure().getFundingCase()) {
                                    case BANK -> request.getExposure().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryLoanWorkflow(outbound0);

        var outbound1 = mapper.submitExposureStage1(request, reply0);
        var reply1 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullUnderwritings(outbound1);

        List<RecallRiskRegisterResponse> results2 = request.getExposure().getLimitsList().stream()
                .map(element -> RecallRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .addChildren(RiskRegister.newBuilder()
                                                .setName(element.getName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallRiskRegister(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? RecallRiskRegisterResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitExposure", request.getExposure().getExposureId());
        return SubmitExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setBalance(request.getExposure().getBalance())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves an exposure.
     */
    public ApproveExposureResponse approveExposure(ApproveExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = SearchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchMetricSerieses(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveExposure", request.getExposure().getExposureId());
        return ApproveExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects an exposure.
     */
    public RejectExposureResponse rejectExposure(RejectExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = LintRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintRiskScenariosRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .build())
                        .addComments(RiskScenarioComment.newBuilder()
                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios", outbound0);
        var reply0 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskScenarios(command0.payload());

        var outbound1 = (ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectExposure", request.getExposure().getExposureId());
        return RejectExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getRiskScenario().getRequester().getName())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setOpenedAt(request.getExposure().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists exposures a page at a time.
     */
    public ListPendingExposuresResponse listPendingExposures(ListPendingExposuresRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingExposures")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Exposure", request.getContext().getTraceId(), "ListPendingExposures");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingExposures", request.getPage().getPageToken());
        return ListPendingExposuresResponse.getDefaultInstance();
    }

    /**
     * Delegates an exposure.
     */
    public DelegateExposureApprovalResponse delegateExposureApproval(DelegateExposureApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateExposureApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = EstimateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            EstimateCustomerResponse fetch(EstimateCustomerRequest input) {
                return customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCustomer(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BrowseRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseRiskRegistersRequest.Filter.newBuilder()
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.RiskRegisterQueryService");
        var selected1 = RiskRegisterQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.browseRiskRegisters(outbound1);

        var outbound2 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply0.getCustomer().getPrimaryAddress().getCountryCode())
                                                .setPostalCode(reply0.getCustomer().getPrimaryAddress().getPostalCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getCustomer().getContact().getEmail())
                                                .setUserId(request.getDetails().getApprover().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<LintRiskRegistersResponse>();
        events.publishEvent(new DelegateExposureApprovalStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateExposureApproval", request.getExposure().getExposureId());
        return DelegateExposureApprovalResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getCustomer().getName())
                                .build())
                        .setExposureId(request.getExposureId())
                        .addLimits(ExposureLimit.newBuilder()
                                .setName(reply2.getRiskRegister().getName())
                                .build())
                        .setStatus(request.getExposure().getStatus())
                        .build())
                .setOutcome(DelegateExposureApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls an exposure.
     */
    public RecallExposureResponse recallExposure(RecallExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getExposure().getAudit().getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .setUserId(request.getDetails().getApprover().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.HouseholdValidationService");
        var serviceName0 = "acme.customers.v1" + "." + "HouseholdValidationService";
        var methodName0 = "Sco" + "reHousehold";
        var descriptor0 = MethodDescriptor.<ScoreHouseholdRequest, ScoreHouseholdResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreHouseholdRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreHouseholdResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getExposure().getLimitsList().stream()
                .map(element -> NormalizeExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .addLimits(ExposureLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setName(element.getName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeExposure(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? NormalizeExposureResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallExposure", request.getExposure().getExposureId());
        return RecallExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getHousehold().getAudit().getCreatedAt())
                                .build())
                        .setAvailable(request.getExposure().getAvailable())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an exposure.
     */
    public GetExposureApprovalStatusResponse getExposureApprovalStatus(GetExposureApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExposureApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Exposure", request.getExposureId(), "GetExposureApprovalStatus");

        var outbound0 = SubmitRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getContext().getDeadline())
                                                .build())
                                        .addChildren(RiskRegister.newBuilder()
                                                .setParentId(request.getExposureId())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskRegisterApprovalService");
        var serviceName0 = "acme.risk.v1" + "." + "RiskRegisterApprovalService";
        var methodName0 = "Sub" + "mitRiskRegister";
        var descriptor0 = MethodDescriptor.<SubmitRiskRegisterRequest, SubmitRiskRegisterResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SubmitRiskRegisterRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubmitRiskRegisterResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ListPendingRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingRiskScenariosRequest.Filter.newBuilder()
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingRiskScenarios(value)))
                .collect(Collectors.toList()).getFirst();
        return GetExposureApprovalStatusResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments an exposure.
     */
    public CommentExposureApprovalResponse commentExposureApproval(CommentExposureApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentExposureApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var results0 = new ArrayList<CheckCustomerSegmentEligibilityResponse>();
        for (var element : request.getExposure().getLimitsList()) {
            CheckCustomerSegmentEligibilityRequest itemRequest = CheckCustomerSegmentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .setName(element.getName())
                        .build())
                .setDetails(CheckCustomerSegmentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .setName(request.getExposure().getContact().getName())
                                .build())
                        .build())
                .build();
            results0.add(customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerSegmentEligibility(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CheckCustomerSegmentEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = GenerateCoverageReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateCoverageReportRequest.Details.newBuilder()
                        .putAllFilters(request.getExposure().getLabelsMap())
                        .setPeriod(reply0.getCustomerSegment().getEffective())
                        .build())
                .setReason(request.getReason())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.CoverageReportingService/GenerateCoverageReport", outbound1);
        var reply1 = insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateCoverageReport(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentExposureApproval", request.getExposure().getExposureId());
        return CommentExposureApprovalResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAvailable(request.getExposure().getAvailable())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
