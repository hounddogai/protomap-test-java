package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BatchGetBankAccountsRequest;
import com.acme.banking.v1.BatchGetBankAccountsResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.lending.v1.CalculateLoanApplicationRequest;
import com.acme.lending.v1.CalculateLoanApplicationResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.EstimateLoanRequest;
import com.acme.lending.v1.EstimateLoanResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanRatesRequest;
import com.acme.lending.v1.GetLoanRatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationBankSettlement;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.workplaces.v1.BuildingQueryServiceGrpc;
import com.acme.workplaces.v1.ExportBuildingsRequest;
import com.acme.workplaces.v1.ExportBuildingsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Computes the figures of a loan, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LoanCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceFutureStub bankingStandingOrderValidationServiceFuture;

    private final ChannelRegistry channels;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final BuildingQueryServiceGrpc.BuildingQueryServiceBlockingStub workplacesBuildingQueryServiceBlocking;

    public LoanCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceFutureStub bankingStandingOrderValidationServiceFuture,
            ChannelRegistry channels,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BuildingQueryServiceGrpc.BuildingQueryServiceBlockingStub workplacesBuildingQueryServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingStandingOrderValidationServiceFuture = bankingStandingOrderValidationServiceFuture;
        this.channels = channels;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.workplacesBuildingQueryServiceBlocking = workplacesBuildingQueryServiceBlocking;
    }

    /**
     * Calculates a loan.
     */
    public CalculateLoanResponse calculateLoan(CalculateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = SimulateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateLoanApplicationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoanApplication(value));

        List<EstimateLoanApplicationResponse> results1 = request.getLoan().getLimitsList().stream()
                .map(element -> EstimateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getLoanApplication().getBillingAddress().getLocality())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateLoanApplication(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EstimateLoanApplicationResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getLoan().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoan().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                .setEtag(request.getLoan().getAudit().getEtag())
                                .setUpdatedAt(request.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateLoan", request.getLoan().getLoanId());
        return CalculateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Estimates a loan.
     */
    public EstimateLoanResponse estimateLoan(EstimateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = StreamPostingBatchNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getLoan().getContact().getPhone())
                        .build())
                .build();
        var stream0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPostingBatchNotices(outbound0);
        var reply0 = StreamPostingBatchNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(CalculateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setHolderName(request.getLoan().getCard().getHolderName())
                                .setToken(request.getLoan().getCard().getToken())
                                .build())
                        .build())
                .build()), CalculateLoanApplicationRequest.class);
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoanApplication(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateLoan", request.getLoan().getLoanId());
        return EstimateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setAvailable(request.getLoan().getAvailable())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Quotes a loan.
     */
    public QuoteLoanResponse quoteLoan(QuoteLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = CompareLoanApplicationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBank(LoanApplicationBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getLoan().getFundingCase()) {
                                    case BANK -> request.getLoan().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLoanApplicationOptions(outbound0);

        var outbound1 = CalculateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateLoanApplicationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLoanApplication().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            CalculateLoanApplicationResponse fetch(CalculateLoanApplicationRequest input) {
                return lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoanApplication(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteLoan", request.getLoan().getLoanId());
        return QuoteLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setCurrencyCode(request.getLoan().getCurrencyCode())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a loan.
     */
    public CompareLoanOptionsResponse compareLoanOptions(CompareLoanOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareLoanOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = RecalculateLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateLoanApplicationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setToken(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoanApplications(outbound0);

        var outbound1 = LintStandingOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLoanApplication().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getLoan().getContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = bankingStandingOrderValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintStandingOrders(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BatchGetBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.banking.v1.BankAccountService");
        var serviceName2 = "acme.banking.v1" + "." + "BankAccountService";
        var methodName2 = "Bat" + "chGetBankAccounts";
        var descriptor2 = MethodDescriptor.<BatchGetBankAccountsRequest, BatchGetBankAccountsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(BatchGetBankAccountsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BatchGetBankAccountsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var futures3 = request.getLoan().getLimitsList().stream()
                .map(element -> EstimateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addLines(LoanApplicationLine.newBuilder()
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateLoanApplication(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? EstimateLoanApplicationResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareLoanOptions", request.getLoan().getLoanId());
        return CompareLoanOptionsResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates a loan.
     */
    public RecalculateLoansResponse recalculateLoans(RecalculateLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var futures0 = request.getLoan().getLimitsList().stream()
                .map(element -> CheckPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .addLimits(PolicyLimit.newBuilder()
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPolicyEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CheckPolicyEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntryVersionsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntryVersions(outbound1));

        var outbound2 = ExportBuildingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBuildingsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setOwnerId(reply0.getPolicy().getHolder().getCoverageId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream2 = workplacesBuildingQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBuildings(outbound2);
        var reply2 = ExportBuildingsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateLoans", request.getLoan().getLoanId());
        return RecalculateLoansResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates a loan.
     */
    public SimulateLoanResponse simulateLoan(SimulateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizePolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getLoan().getContact().getPhone())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setToken(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getLoan().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateLoan", request.getLoan().getLoanId());
        return SimulateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Explains a loan.
     */
    public ExplainLoanCalculationResponse explainLoanCalculation(ExplainLoanCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLoanCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = ExplainPolicyDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ExplainPolicyDecisionRequest, ExplainPolicyDecisionResponse>() {
            @Override
            public ExplainPolicyDecisionResponse apply(ExplainPolicyDecisionRequest input) {
                return insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyDecision(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RecalculateLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LoanApplicationBankSettlement.newBuilder()
                                .setIban(switch (request.getLoan().getFundingCase()) {
                                    case BANK -> request.getLoan().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoanApplications(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLoanCalculation", request.getLoan().getLoanId());
        return ExplainLoanCalculationResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setContact(request.getDetails().getContact())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public GetLoanRatesResponse getLoanRates(GetLoanRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "GetLoanRates");
        return GetLoanRatesResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }
}
