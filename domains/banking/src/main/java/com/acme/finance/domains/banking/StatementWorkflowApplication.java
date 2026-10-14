package com.acme.finance.domains.banking;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.banking.v1.AdvanceStatementWorkflowRequest;
import com.acme.banking.v1.AdvanceStatementWorkflowResponse;
import com.acme.banking.v1.ApproveStatementRequest;
import com.acme.banking.v1.ApproveStatementResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.CancelStatementWorkflowRequest;
import com.acme.banking.v1.CancelStatementWorkflowResponse;
import com.acme.banking.v1.InspectStatementWorkflowRequest;
import com.acme.banking.v1.InspectStatementWorkflowResponse;
import com.acme.banking.v1.ListStatementWorkflowsRequest;
import com.acme.banking.v1.ListStatementWorkflowsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PauseStatementWorkflowRequest;
import com.acme.banking.v1.PauseStatementWorkflowResponse;
import com.acme.banking.v1.RecallStandingOrderRequest;
import com.acme.banking.v1.RecallStandingOrderResponse;
import com.acme.banking.v1.RejectStatementRequest;
import com.acme.banking.v1.RejectStatementResponse;
import com.acme.banking.v1.ResumeBankAccountWorkflowRequest;
import com.acme.banking.v1.ResumeBankAccountWorkflowResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.RetryStatementWorkflowRequest;
import com.acme.banking.v1.RetryStatementWorkflowResponse;
import com.acme.banking.v1.SignalStatementWorkflowRequest;
import com.acme.banking.v1.SignalStatementWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StartStatementWorkflowRequest;
import com.acme.banking.v1.StartStatementWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementAttachment;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementLine;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.catalog.v1.BatchGetProductsRequest;
import com.acme.catalog.v1.BatchGetProductsResponse;
import com.acme.catalog.v1.ProductServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerDecisionRequest;
import com.acme.customers.v1.ExplainCustomerDecisionResponse;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
 * Runs the long-lived workflow that takes a statement from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StatementWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(StatementWorkflowApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceFutureStub bankingBankAccountWorkflowServiceFuture;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking;

    private final ProductServiceGrpc.ProductServiceBlockingStub catalogProductServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceFutureStub ledgerJournalEntryNotificationServiceFuture;

    private final BankingRepository repository;

    private final Executor rpcExecutor;

    public StatementWorkflowApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceFutureStub bankingBankAccountWorkflowServiceFuture,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking,
            ProductServiceGrpc.ProductServiceBlockingStub catalogProductServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceFutureStub ledgerJournalEntryNotificationServiceFuture,
            BankingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingBankAccountWorkflowServiceFuture = bankingBankAccountWorkflowServiceFuture;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.bankingStatementApprovalServiceBlocking = bankingStatementApprovalServiceBlocking;
        this.catalogProductServiceBlocking = catalogProductServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.ledgerJournalEntryNotificationServiceFuture = ledgerJournalEntryNotificationServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a statement.
     */
    public StartStatementWorkflowResponse startStatementWorkflow(StartStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = ListJournalEntrySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntrySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStatement().getAudit().getCreatedAt())
                        .setOwnerId(request.getStatement().getCounterparty().getStandingOrderId())
                        .setRegion(request.getStatement().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = ledgerJournalEntryNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntrySubscriptions(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartStatementWorkflow", request.getStatement().getStatementId());
        return StartStatementWorkflowResponse.newBuilder()
                .setOutcome(StartStatementWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setNumber(request.getStatement().getNumber())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Advances a statement.
     */
    public AdvanceStatementWorkflowResponse advanceStatementWorkflow(AdvanceStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = StreamUnderwritingMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamUnderwritingMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getStatement().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingMetrics(outbound0);
        var reply0 = StreamUnderwritingMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        List<RejectStatementResponse> results1 = request.getStatement().getAttachmentsList().stream()
                .map(element -> RejectStatementRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .addAttachments(StatementAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectStatement(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? RejectStatementResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BeanCopies.copy(SuggestFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getStatement().getCounterparty().getStartsAt())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .setRegion(reply1.getStatement().getBillingAddress().getRegion())
                        .build())
                .build(), SuggestFeatureFlagsRequest.class);
        var reply2 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestFeatureFlags(outbound2);

        var outbound3 = QuoteCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .setEtag(request.getStatement().getAudit().getEtag())
                                .build())
                        .build())
                .setDetails(QuoteCustomerRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getStatement().getBillingAddress().getCountryCode())
                                .setPostalCode(reply1.getStatement().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<QuoteCustomerRequest, QuoteCustomerResponse>() {
            @Override
            public QuoteCustomerResponse apply(QuoteCustomerRequest input) {
                return customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomer(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceStatementWorkflow", request.getStatement().getStatementId());
        return AdvanceStatementWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setStatement(Statement.newBuilder()
                        .addLines(StatementLine.newBuilder()
                                .putAllAttributes(reply3.getCustomer().getAttributesMap())
                                .build())
                        .setNotes(request.getStatement().getNotes())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Cancels a statement.
     */
    public CancelStatementWorkflowResponse cancelStatementWorkflow(CancelStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStatement().getDueAt())
                        .setRegion(request.getStatement().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.CustomerQueryService");
        var serviceName0 = "acme.customers.v1" + "." + "CustomerQueryService";
        var methodName0 = "Fin" + "dCustomers";
        var descriptor0 = MethodDescriptor.<FindCustomersRequest, FindCustomersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(FindCustomersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(FindCustomersResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ExportCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStatement().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getStatement().getStatementId())
                        .setRegion(request.getStatement().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCustomers(outbound1);
        var reply1 = ExportCustomersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getStatement().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelStatementWorkflow", request.getStatement().getNumber());
        return CancelStatementWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setCounterparty(request.getStatement().getCounterparty())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Retrys a statement.
     */
    public RetryStatementWorkflowResponse retryStatementWorkflow(RetryStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = BatchGetProductsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> catalogProductServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetProducts(outbound0), rpcExecutor);

        var outbound1 = CompareHouseholdOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getStatement().getAudit().getEtag())
                                                .setUpdatedAt(request.getStatement().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getStatement().getBillingAddress().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotify().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareHouseholdOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryStatementWorkflow", request.getStatement().getStatementId());
        return RetryStatementWorkflowResponse.newBuilder()
                .setOutcome(RetryStatementWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setNotes(request.getStatement().getNotes())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Pauses a statement.
     */
    public PauseStatementWorkflowResponse pauseStatementWorkflow(PauseStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var results0 = new ArrayList<CalculateMandateResponse>();
        for (var element : request.getStatement().getLinesList()) {
            CalculateMandateRequest itemRequest = CalculateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setQuantity(element.getQuantity())
                                .setTax(MandateLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateMandate(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CalculateMandateResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecallStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.banking.v1.StandingOrderApprovalService/RecallStandingOrder", outbound1);
        var reply1 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallStandingOrder(command1.payload());

        var outbound2 = ExplainCustomerDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getStatement().getAudit().getUpdatedAt())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getStatement().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = ResumeBankAccountWorkflowRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getStandingOrder().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending3 = bankingBankAccountWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resumeBankAccountWorkflow(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseStatementWorkflow", request.getStatement().getStatementId());
        return PauseStatementWorkflowResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getStandingOrder().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getStandingOrder().getAudit().getCreatedBy())
                                .setUpdatedAt(reply3.getBankAccount().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getCustomer().getPrimaryAddress().getLocality())
                                .build())
                        .addLines(StatementLine.newBuilder()
                                .putAllAttributes(reply2.getCustomer().getAttributesMap())
                                .build())
                        .setStatementId(request.getStatementId())
                        .setTotal(request.getStatement().getTotal())
                        .build())
                .build();
    }

    /**
     * Resumes a statement.
     */
    public ResumeStatementWorkflowResponse resumeStatementWorkflow(ResumeStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = NormalizeMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeMandate(outbound0);

        var outbound1 = ApproveStatementRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getStatement().getAudit().getUpdatedBy())
                                .build())
                        .setCard(StatementCardSettlement.newBuilder()
                                .setLast4(request.getStatement().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ApproveStatementRequest, ApproveStatementResponse>() {
            @Override
            public ApproveStatementResponse apply(ApproveStatementRequest input) {
                return bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveStatement(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = JsonCodec.roundTrip(QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getStatement().getBillingAddress().getLinesList())
                                .setRecipient(request.getStatement().getBillingAddress().getRecipient())
                                .setRegion(reply1.getStatement().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), QuotePreferenceRequest.class);
        var reply2 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeStatementWorkflow", request.getStatement().getStatementId());
        return ResumeStatementWorkflowResponse.newBuilder()
                .setOutcome(ResumeStatementWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getPreference().getAudit().getCreatedAt())
                                .setUpdatedBy(reply2.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getMandate().getBillingAddress().getLocality())
                                .build())
                        .putAllMetadata(request.getStatement().getMetadataMap())
                        .setStatementId(request.getStatementId())
                        .setTotal(reply1.getStatement().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a statement.
     */
    public InspectStatementWorkflowResponse inspectStatementWorkflow(InspectStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getStatementId(), "InspectStatementWorkflow");

        var outbound0 = JsonCodec.roundTrip(RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSource(request.getStatementId())
                        .build())
                .setContext(request.getContext())
                .build(), RecalculateConsentRecordsRequest.class);
        var reply0 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConsentRecords(outbound0);

        var outbound1 = CalculateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomerSegment(value)).orElseThrow();
        return InspectStatementWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCustomerSegment().getAudit().getCreatedBy())
                                .build())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Lists statements a page at a time.
     */
    public ListStatementWorkflowsResponse listStatementWorkflows(ListStatementWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListStatementWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "ListStatementWorkflows");

        var outbound0 = ListPendingClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingClaimsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingClaims(outbound0), rpcExecutor);

        var outbound1 = CreateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAccessPolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListStatementWorkflows", request.getPage().getPageToken());
        return ListStatementWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .addStatements(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getAccessPolicy().getAudit().getCreatedBy())
                                .build())
                        .addLines(StatementLine.newBuilder()
                                .setAmount(reply1.getAccessPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a statement.
     */
    public SignalStatementWorkflowResponse signalStatementWorkflow(SignalStatementWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalStatementWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var results0 = new ArrayList<SummarizeBankAccountsResponse>();
        for (var element : request.getStatement().getLinesList()) {
            SummarizeBankAccountsRequest itemRequest = SummarizeBankAccountsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeBankAccountsRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build();
            results0.add(bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeBankAccounts(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SummarizeBankAccountsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalStatementWorkflow", request.getStatement().getStatementId());
        return SignalStatementWorkflowResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setDueAt(request.getStatement().getDueAt())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }
}
