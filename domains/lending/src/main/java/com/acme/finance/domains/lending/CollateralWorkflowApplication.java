package com.acme.finance.domains.lending;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.EscalatePremiumRequest;
import com.acme.insurance.v1.EscalatePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.lending.v1.AdvanceCollateralWorkflowRequest;
import com.acme.lending.v1.AdvanceCollateralWorkflowResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.CommentCollateralApprovalRequest;
import com.acme.lending.v1.CommentCollateralApprovalResponse;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseCollateralWorkflowRequest;
import com.acme.lending.v1.PauseCollateralWorkflowResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.RejectLoanApplicationRequest;
import com.acme.lending.v1.RejectLoanApplicationResponse;
import com.acme.lending.v1.RejectRepaymentRequest;
import com.acme.lending.v1.RejectRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ResumeCollateralWorkflowRequest;
import com.acme.lending.v1.ResumeCollateralWorkflowResponse;
import com.acme.lending.v1.SignalCollateralWorkflowRequest;
import com.acme.lending.v1.SignalCollateralWorkflowResponse;
import com.acme.lending.v1.StartCollateralWorkflowRequest;
import com.acme.lending.v1.StartCollateralWorkflowResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.payments.v1.CalculatePaymentRequest;
import com.acme.payments.v1.CalculatePaymentResponse;
import com.acme.payments.v1.Payment;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a collateral from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CollateralWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CollateralWorkflowApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ApplicationEventPublisher events;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final LendingRepository repository;

    public CollateralWorkflowApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking,
            ChannelRegistry channels,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ApplicationEventPublisher events,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            LendingRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.boxofficeBoxOfficeSaleCalculationServiceBlocking = boxofficeBoxOfficeSaleCalculationServiceBlocking;
        this.channels = channels;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.events = events;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
    }

    public record ResumeCollateralWorkflowStage1Requested(CalculatePaymentRequest request, CompletableFuture<CalculatePaymentResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a collateral.
     */
    public StartCollateralWorkflowResponse startCollateralWorkflow(StartCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = RejectRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getCollateral().getAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RejectRepaymentResponse fetch(RejectRepaymentRequest input) {
                return lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRepayment(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getCollateral().getCollateralId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input1 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCollateral().getAudit().getCreatedBy())
                                .setEtag(request.getCollateral().getAudit().getEtag())
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCollateralWorkflow", request.getCollateral().getCode());
        return StartCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .setCapacity(request.getCollateral().getCapacity())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(StartCollateralWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a collateral.
     */
    public AdvanceCollateralWorkflowResponse advanceCollateralWorkflow(AdvanceCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = EscalatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .setRegion(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalatePremium(outbound0);

        var outbound1 = (CancelLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelLoanWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .build())
                        .setCard(LoanCardFunding.newBuilder()
                                .setHolderName(reply0.getPremium().getCard().getHolderName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelLoanWorkflow(outbound1);

        var outbound2 = ApproveLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getLoan().getAudit().getEtag())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setToken(reply1.getLoan().getCard().getToken())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.lending.v1.LoanApplicationApprovalService/ApproveLoanApplication", outbound2);
        var reply2 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveLoanApplication(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCollateralWorkflow", request.getCollateral().getCollateralId());
        return AdvanceCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .setStatus(request.getCollateral().getStatus())
                        .build())
                .setOutcome(AdvanceCollateralWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a collateral.
     */
    public CancelCollateralWorkflowResponse cancelCollateralWorkflow(CancelCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = ShareCoverageReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareCoverageReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.CoverageReportingService");
        var serviceName0 = "acme.insurance.v1" + "." + "CoverageReportingService";
        var methodName0 = "Sha" + "reCoverageReport";
        var descriptor0 = MethodDescriptor.<ShareCoverageReportRequest, ShareCoverageReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ShareCoverageReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ShareCoverageReportResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelCollateralWorkflow", request.getCollateral().getCollateralId());
        return CancelCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(CancelCollateralWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Pauses a collateral.
     */
    public PauseCollateralWorkflowResponse pauseCollateralWorkflow(PauseCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = ScoreMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreMetricSeriesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getCollateral().getAddress().getLinesList())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ScoreMetricSeriesResponse fetch(ScoreMetricSeriesRequest input) {
                return metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMetricSeries(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseCollateralWorkflow", request.getCollateral().getCollateralId());
        return PauseCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .setDimensions(CollateralDimensions.newBuilder()
                                .setHeight(reply0.getMetricSeries().getDimensions().getHeight())
                                .build())
                        .build())
                .build();
    }

    /**
     * Resumes a collateral.
     */
    public ResumeCollateralWorkflowResponse resumeCollateralWorkflow(ResumeCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = CommentRepaymentApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CommentRepaymentApprovalResponse fetch(CommentRepaymentApprovalRequest input) {
                return lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentRepaymentApproval(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = CalculatePaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePaymentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getCollateral().getAddress().getRecipient())
                                .build())
                        .build())
                .setPayment(Payment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRepayment().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CalculatePaymentResponse>();
        events.publishEvent(new ResumeCollateralWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCollateralWorkflow", request.getCollateral().getCollateralId());
        return ResumeCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRepayment().getAudit().getCreatedAt())
                                .build())
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPayment().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ResumeCollateralWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists collaterals a page at a time.
     */
    public ListCollateralWorkflowsResponse listCollateralWorkflows(ListCollateralWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCollateralWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getContext().getTraceId(), "ListCollateralWorkflows");

        var outbound0 = QuoteRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.RepaymentCalculationService");
        var serviceName0 = "acme.lending.v1" + "." + "RepaymentCalculationService";
        var methodName0 = "Quo" + "teRepayment";
        var descriptor0 = MethodDescriptor.<QuoteRepaymentRequest, QuoteRepaymentResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteRepaymentRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteRepaymentResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = RejectLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectLoanApplicationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getRepayment().getOrganizer().getUserId())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.LoanApplicationApprovalService");
        var serviceName1 = "acme.lending.v1" + "." + "LoanApplicationApprovalService";
        var methodName1 = "Rej" + "ectLoanApplication";
        var descriptor1 = MethodDescriptor.<RejectLoanApplicationRequest, RejectLoanApplicationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(RejectLoanApplicationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RejectLoanApplicationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCollateralWorkflows", request.getPage().getPageToken());
        return ListCollateralWorkflowsResponse.newBuilder()
                .addCollaterals(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRepayment().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a collateral.
     */
    public SignalCollateralWorkflowResponse signalCollateralWorkflow(SignalCollateralWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCollateralWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = CommentCollateralApprovalRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.CollateralApprovalService");
        var selected0 = CollateralApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.commentCollateralApproval(outbound0);

        var outbound1 = RecalculateBoxOfficeSalesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateBoxOfficeSalesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getCountryCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = boxofficeBoxOfficeSaleCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBoxOfficeSales(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCollateralWorkflow", request.getCollateral().getCollateralId());
        return SignalCollateralWorkflowResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBoxOfficeSale().getAudit().getCreatedBy())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(request.getCollateral().getOwner())
                        .putAllSpecs(reply0.getCollateral().getSpecsMap())
                        .build())
                .setOutcome(SignalCollateralWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
