package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.SuggestBenefitPlansRequest;
import com.acme.benefits.v1.SuggestBenefitPlansResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.ListPendingCoveragesRequest;
import com.acme.insurance.v1.ListPendingCoveragesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.labeling.v1.ExplainLabelingTaskCalculationRequest;
import com.acme.labeling.v1.ExplainLabelingTaskCalculationResponse;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.lending.v1.ApproveCollateralRequest;
import com.acme.lending.v1.ApproveCollateralResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralLifecycleServiceGrpc;
import com.acme.lending.v1.CommentCollateralApprovalRequest;
import com.acme.lending.v1.CommentCollateralApprovalResponse;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.DelegateCollateralApprovalRequest;
import com.acme.lending.v1.DelegateCollateralApprovalResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.EscalateCollateralRequest;
import com.acme.lending.v1.EscalateCollateralResponse;
import com.acme.lending.v1.GetCollateralApprovalStatusRequest;
import com.acme.lending.v1.GetCollateralApprovalStatusResponse;
import com.acme.lending.v1.ListPendingCollateralsRequest;
import com.acme.lending.v1.ListPendingCollateralsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.NormalizeRepaymentRequest;
import com.acme.lending.v1.NormalizeRepaymentResponse;
import com.acme.lending.v1.RecallCollateralRequest;
import com.acme.lending.v1.RecallCollateralResponse;
import com.acme.lending.v1.RejectCollateralRequest;
import com.acme.lending.v1.RejectCollateralResponse;
import com.acme.lending.v1.RenewCollateralRequest;
import com.acme.lending.v1.RenewCollateralResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.SubmitCollateralRequest;
import com.acme.lending.v1.SubmitCollateralResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
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
 * Routes collaterals through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CollateralApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(CollateralApprovalApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking;

    private final BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final CollateralLifecycleServiceGrpc.CollateralLifecycleServiceBlockingStub lendingCollateralLifecycleServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final CollateralApprovalRequestMapper mapper;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public CollateralApprovalApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking,
            BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            CollateralLifecycleServiceGrpc.CollateralLifecycleServiceBlockingStub lendingCollateralLifecycleServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            CollateralApprovalRequestMapper mapper,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.bankingStandingOrderWorkflowServiceBlocking = bankingStandingOrderWorkflowServiceBlocking;
        this.benefitsBenefitPlanQueryServiceBlocking = benefitsBenefitPlanQueryServiceBlocking;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.labelingLabelingTaskCalculationServiceBlocking = labelingLabelingTaskCalculationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingCollateralLifecycleServiceBlocking = lendingCollateralLifecycleServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    /**
     * Submits a collateral.
     */
    public SubmitCollateralResponse submitCollateral(SubmitCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = RejectCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCoverage(outbound0), rpcExecutor);

        var outbound1 = CountRepaymentTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRepaymentTotalsRequest.Filter.newBuilder()
                        .setRegion(request.getCollateral().getAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRepaymentTotals(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (CancelLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getCollateral().getAddress().getLinesList())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelLoanApplicationWorkflow(outbound2);

        var outbound3 = RenewCollateralRequest.newBuilder()
                .setCollateralId(request.getCollateral().getCollateralId())
                .setContext(request.getContext())
                .setDetails(RenewCollateralRequest.Details.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingCollateralLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).renewCollateral(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitCollateral", request.getCollateral().getCode());
        return SubmitCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getLoanApplication().getAudit().getCreatedAt())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setName(request.getCollateral().getName())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getCoverage().getOwner().getUserId())
                                .build())
                        .build())
                .setOutcome(SubmitCollateralResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Approves a collateral.
     */
    public ApproveCollateralResponse approveCollateral(ApproveCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = RpcSupport.unpack(Any.pack(NormalizeRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getCollateral().getAddress().getLinesList())
                                .build())
                        .build())
                .build()), NormalizeRepaymentRequest.class);
        var reply0 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRepayment(outbound0);

        var outbound1 = ListPendingPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getCreatedAt())
                        .putAllLabels(request.getCollateral().getSpecsMap())
                        .setOwnerId(reply0.getRepayment().getRepaymentId())
                        .setRegion(switch (request.getCollateral().getLocatorCase()) {
                            case ADDRESS -> request.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPaymentPromises(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveCollateral", request.getCollateral().getCollateralId());
        return ApproveCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(ApproveCollateralResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Rejects a collateral.
     */
    public RejectCollateralResponse rejectCollateral(RejectCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = ApproveCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.CoverageApprovalService/ApproveCoverage", outbound0);
        var reply0 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveCoverage(command0.payload());

        var outbound1 = NormalizeAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addLines(AuditTrailLine.newBuilder()
                                .setDescription(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeAuditTrailRequest, NormalizeAuditTrailResponse> port1 = RpcPort.adapt(auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeAuditTrail);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = mapper.rejectCollateralStage2(request, reply1);
        var reply2 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoan(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectCollateral", request.getCollateral().getCollateralId());
        return RejectCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getAuditTrail().getAudit().getEtag())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getLoan().getHolder().getDisplayName())
                                .setEmail(reply2.getLoan().getContact().getEmail())
                                .build())
                        .setStatus(request.getCollateral().getStatus())
                        .build())
                .setOutcome(RejectCollateralResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a collateral.
     */
    public EscalateCollateralResponse escalateCollateral(EscalateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = SuggestBenefitPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBenefitPlansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getCreatedAt())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(switch (request.getCollateral().getLocatorCase()) {
                            case ADDRESS -> request.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = benefitsBenefitPlanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBenefitPlans(outbound0);

        var outbound1 = BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getCollateral().getSpecsMap())
                        .setOwnerId(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getComponentId())
                        .setRegion(switch (request.getCollateral().getLocatorCase()) {
                            case ADDRESS -> request.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateCollateral", request.getCollateral().getCollateralId());
        return EscalateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .putAllSpecs(request.getCollateral().getSpecsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists collaterals a page at a time.
     */
    public ListPendingCollateralsResponse listPendingCollaterals(ListPendingCollateralsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingCollaterals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getContext().getTraceId(), "ListPendingCollaterals");

        var outbound0 = (PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingCollaterals", request.getPage().getPageToken());
        return ListPendingCollateralsResponse.newBuilder()
                .addCollaterals(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a collateral.
     */
    public DelegateCollateralApprovalResponse delegateCollateralApproval(DelegateCollateralApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateCollateralApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = ExplainLabelingTaskCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainLabelingTaskCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setLabelingTask(LabelingTask.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCollateral().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ExplainLabelingTaskCalculationRequest, ExplainLabelingTaskCalculationResponse>() {
            @Override
            public ExplainLabelingTaskCalculationResponse apply(ExplainLabelingTaskCalculationRequest input) {
                return labelingLabelingTaskCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLabelingTaskCalculation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ListPendingCoveragesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingCoveragesRequest.Filter.newBuilder()
                        .setRegion(switch (request.getCollateral().getLocatorCase()) {
                            case ADDRESS -> request.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingCoverages(outbound1);

        var outbound2 = EscalatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalatePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLabelingTask().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalatePolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateCollateralApproval", request.getCollateral().getCollateralId());
        return DelegateCollateralApprovalResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getLabelingTask().getAudit().getEtag())
                                .build())
                        .setCapacity(request.getCollateral().getCapacity())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getLabelingTask().getAssignee().getEmail())
                                .build())
                        .build())
                .setOutcome(DelegateCollateralApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalls a collateral.
     */
    public RecallCollateralResponse recallCollateral(RecallCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = RpcSupport.unpack(Any.pack(CountCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getUpdatedAt())
                        .setOwnerId(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getComponentId())
                        .setRegion(request.getCollateral().getAddress().getRegion())
                        .build())
                .build()), CountCustomersRequest.class);
        var reply0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCustomers(outbound0);

        var outbound1 = DelegateLoanApplicationApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegateLoanApplicationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getCollateral().getOwner().getDisplayName())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateLoanApplicationApproval(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallCollateral", request.getCollateral().getCollateralId());
        return RecallCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(RecallCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a collateral.
     */
    public GetCollateralApprovalStatusResponse getCollateralApprovalStatus(GetCollateralApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollateralApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "GetCollateralApprovalStatus");

        var outbound0 = mapper.getCollateralApprovalStatusStage0(request);
        var reply0 = bankingStandingOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseStandingOrderWorkflow(outbound0);
        return GetCollateralApprovalStatusResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .setUpdatedBy(reply0.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .build();
    }

    /**
     * Comments a collateral.
     */
    public CommentCollateralApprovalResponse commentCollateralApproval(CommentCollateralApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentCollateralApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = StreamJournalEntryNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamJournalEntryNotices(outbound0);
        var reply0 = StreamJournalEntryNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = EstimateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCollateral().getAudit().getEtag())
                                .setUpdatedBy(request.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRecipient(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCustomer(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentCollateralApproval", request.getCollateral().getCollateralId());
        return CommentCollateralApprovalResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
