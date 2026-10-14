package com.acme.finance.domains.lending;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.StartBankAccountWorkflowRequest;
import com.acme.banking.v1.StartBankAccountWorkflowResponse;
import com.acme.banking.v1.StartStatementWorkflowRequest;
import com.acme.banking.v1.StartStatementWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.CloneForumRequest;
import com.acme.community.v1.CloneForumResponse;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.concessions.v1.ConcessionStandServiceGrpc;
import com.acme.concessions.v1.SearchConcessionStandsRequest;
import com.acme.concessions.v1.SearchConcessionStandsResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.customers.v1.ValidateCustomerRequest;
import com.acme.customers.v1.ValidateCustomerResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.ApproveCollateralRequest;
import com.acme.lending.v1.ApproveCollateralResponse;
import com.acme.lending.v1.ApproveRepaymentRequest;
import com.acme.lending.v1.ApproveRepaymentResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.EscalateRepaymentRequest;
import com.acme.lending.v1.EscalateRepaymentResponse;
import com.acme.lending.v1.GetRepaymentApprovalStatusRequest;
import com.acme.lending.v1.GetRepaymentApprovalStatusResponse;
import com.acme.lending.v1.ListPendingRepaymentsRequest;
import com.acme.lending.v1.ListPendingRepaymentsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.RecallRepaymentRequest;
import com.acme.lending.v1.RecallRepaymentResponse;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.lending.v1.RejectRepaymentRequest;
import com.acme.lending.v1.RejectRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.licensing.v1.EstimateSoftwareLicenseRequest;
import com.acme.licensing.v1.EstimateSoftwareLicenseResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
import java.util.List;
import java.util.Optional;
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
 * Routes repayments through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RepaymentApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(RepaymentApprovalApplication.class);

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking;

    private final ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking;

    private final ConcessionStandServiceGrpc.ConcessionStandServiceBlockingStub concessionsConcessionStandServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final CollateralApprovalServiceGrpc.CollateralApprovalServiceBlockingStub lendingCollateralApprovalServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final SoftwareLicenseCalculationServiceGrpc.SoftwareLicenseCalculationServiceBlockingStub licensingSoftwareLicenseCalculationServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    public RepaymentApprovalApplication(
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking,
            ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking,
            ConcessionStandServiceGrpc.ConcessionStandServiceBlockingStub concessionsConcessionStandServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            CollateralApprovalServiceGrpc.CollateralApprovalServiceBlockingStub lendingCollateralApprovalServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            SoftwareLicenseCalculationServiceGrpc.SoftwareLicenseCalculationServiceBlockingStub licensingSoftwareLicenseCalculationServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.bankingStatementWorkflowServiceBlocking = bankingStatementWorkflowServiceBlocking;
        this.communityForumServiceBlocking = communityForumServiceBlocking;
        this.concessionsConcessionStandServiceBlocking = concessionsConcessionStandServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.insuranceCoverageLifecycleServiceBlocking = insuranceCoverageLifecycleServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.lendingCollateralApprovalServiceBlocking = lendingCollateralApprovalServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.licensingSoftwareLicenseCalculationServiceBlocking = licensingSoftwareLicenseCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a repayment.
     */
    public SubmitRepaymentResponse submitRepayment(SubmitRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = StartBankAccountWorkflowRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setHolder(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<StartBankAccountWorkflowRequest, StartBankAccountWorkflowResponse> port0 = RpcPort.adapt(bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::startBankAccountWorkflow);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = PreviewCollateralRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewCollateralRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getBankAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            PreviewCollateralResponse fetch(PreviewCollateralRequest input) {
                return lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCollateral(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        List<EstimateCustomerResponse> results2 = request.getRepayment().getParticipantsList().stream()
                .map(element -> EstimateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCustomer(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? EstimateCustomerResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = StartStatementWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartStatementWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(reply2.getCustomer().getContact().getPhone())
                                .setPreferredChannel(reply2.getCustomer().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            StartStatementWorkflowResponse fetch(StartStatementWorkflowRequest input) {
                return bankingStatementWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startStatementWorkflow(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitRepayment", request.getRepayment().getRepaymentId());
        return SubmitRepaymentResponse.newBuilder()
                .setOutcome(SubmitRepaymentResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getBankAccount().getContact().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Approves a repayment.
     */
    public ApproveRepaymentResponse approveRepayment(ApproveRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = CancelLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRepayment().getAudit().getCreatedAt())
                                .setEtag(request.getRepayment().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelLoanWorkflow(value)).orElseThrow();

        var outbound1 = ValidateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getLoan().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ValidateCustomerResponse fetch(ValidateCustomerRequest input) {
                return customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCustomer(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        List<CheckHouseholdEligibilityResponse> results2 = request.getRepayment().getParticipantsList().stream()
                .map(element -> CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckHouseholdEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(element.getContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckHouseholdEligibilityResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveRepayment", request.getRepayment().getRepaymentId());
        return ApproveRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .putAllMetadata(request.getRepayment().getMetadataMap())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply1.getCustomer().getContact().getPhone())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a repayment.
     */
    public RejectRepaymentResponse rejectRepayment(RejectRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var futures0 = request.getRepayment().getParticipantsList().stream()
                .map(element -> EstimateSoftwareLicenseRequest.newBuilder()
                .setContext(request.getContext())
                .setSoftwareLicense(SoftwareLicense.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> licensingSoftwareLicenseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateSoftwareLicense(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? EstimateSoftwareLicenseResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PauseLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRepayment().getAudit().getEtag())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(reply0.getSoftwareLicense().getHolder().getNumber())
                                .build())
                        .build())
                .build();
        Function<PauseLoanWorkflowRequest, PauseLoanWorkflowResponse> operation1 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pauseLoanWorkflow;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectRepayment", request.getRepayment().getRepaymentId());
        return RejectRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a repayment.
     */
    public EscalateRepaymentResponse escalateRepayment(EscalateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = JsonCodec.roundTrip(RecallLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallLoanApplicationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getRepayment().getOrganizer().getEmail())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRepayment().getLocation().getPostalCode())
                                .build())
                        .build())
                .build(), RecallLoanApplicationRequest.class);
        var reply0 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallLoanApplication(outbound0);

        var outbound1 = SuggestHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getLoanApplication().getAudit().getUpdatedAt())
                        .setRegion(request.getRepayment().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestHouseholds(value)).orElseThrow();

        var outbound2 = StartLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartLoanApplicationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getRepayment().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startLoanApplicationWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateRepayment", request.getRepayment().getRepaymentId());
        return EscalateRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply2.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .putAllMetadata(reply0.getLoanApplication().getMetadataMap())
                        .setRepaymentId(request.getRepaymentId())
                        .setTitle(request.getRepayment().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists repayments a page at a time.
     */
    public ListPendingRepaymentsResponse listPendingRepayments(ListPendingRepaymentsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingRepayments")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getContext().getTraceId(), "ListPendingRepayments");

        var outbound0 = CloneForumRequest.newBuilder()
                .setContext(request.getContext())
                .setForum(Forum.newBuilder()
                        .addChildren(Forum.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .setRegion(request.getFilter().getRegion())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> communityForumServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneForum(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingRepayments", request.getPage().getPageToken());
        return ListPendingRepaymentsResponse.newBuilder()
                .addRepayments(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getForum().getLocation().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getForum().getManager().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Recalls a repayment.
     */
    public RecallRepaymentResponse recallRepayment(RecallRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        List<QuoteCustomerSegmentResponse> results0 = request.getRepayment().getParticipantsList().stream()
                .map(element -> QuoteCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(element.getContact().getEmail())
                                .setUserId(request.getRepayment().getOrganizer().getUserId())
                                .build())
                        .build())
                .setDetails(QuoteCustomerSegmentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getRepayment().getLocation().getPostalCode())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomerSegment(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? QuoteCustomerSegmentResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolveHouseholdsRequest, ResolveHouseholdsResponse> port1 = RpcPort.adapt(customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveHouseholds);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = StartLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRepayment().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getRepayment().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startLoanApplicationWorkflow(outbound2);

        var futures3 = request.getRepayment().getParticipantsList().stream()
                .map(element -> SearchConcessionStandsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchConcessionStandsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCustomerSegment().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(element.getParticipantId())
                        .setRegion(reply2.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> concessionsConcessionStandServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchConcessionStands(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? SearchConcessionStandsResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallRepayment", request.getRepayment().getRepaymentId());
        return RecallRepaymentResponse.newBuilder()
                .setOutcome(RecallRepaymentResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(request.getRepayment().getLocation())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a repayment.
     */
    public GetRepaymentApprovalStatusResponse getRepaymentApprovalStatus(GetRepaymentApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRepaymentApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "GetRepaymentApprovalStatus");

        var outbound0 = ScheduleCoverageExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addAllScopes(request.getContext().getActor().getRolesList())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCoverageExpiry(value)).orElseThrow();

        var outbound1 = ApproveCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> lendingCollateralApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveCollateral(value));
        return GetRepaymentApprovalStatusResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCollateral().getAddress().getLinesList())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getCollateral().getOwner().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a repayment.
     */
    public CommentRepaymentApprovalResponse commentRepaymentApproval(CommentRepaymentApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentRepaymentApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = RevertJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertJournalEntryRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getRepayment().getLocation().getCountryCode())
                                .build())
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertJournalEntry(outbound0);

        var outbound1 = RejectLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectLoanRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRepayment().getAudit().getEtag())
                                .setUpdatedBy(reply0.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply0.getJournalEntry().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectLoan(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentRepaymentApproval", request.getRepayment().getRepaymentId());
        return CommentRepaymentApprovalResponse.newBuilder()
                .setOutcome(CommentRepaymentApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(request.getRepayment().getLocation())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply1.getLoan().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
