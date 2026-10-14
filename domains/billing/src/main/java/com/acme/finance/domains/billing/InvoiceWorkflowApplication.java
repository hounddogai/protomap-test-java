package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.billing.v1.AdvanceInvoiceWorkflowRequest;
import com.acme.billing.v1.AdvanceInvoiceWorkflowResponse;
import com.acme.billing.v1.ApproveCreditNoteRequest;
import com.acme.billing.v1.ApproveCreditNoteResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.CancelInvoiceWorkflowRequest;
import com.acme.billing.v1.CancelInvoiceWorkflowResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.GetInvoiceApprovalStatusRequest;
import com.acme.billing.v1.GetInvoiceApprovalStatusResponse;
import com.acme.billing.v1.InspectInvoiceWorkflowRequest;
import com.acme.billing.v1.InspectInvoiceWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListInvoiceWorkflowsRequest;
import com.acme.billing.v1.ListInvoiceWorkflowsResponse;
import com.acme.billing.v1.ListPendingCreditNotesRequest;
import com.acme.billing.v1.ListPendingCreditNotesResponse;
import com.acme.billing.v1.PauseInvoiceWorkflowRequest;
import com.acme.billing.v1.PauseInvoiceWorkflowResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.ResumeInvoiceWorkflowRequest;
import com.acme.billing.v1.ResumeInvoiceWorkflowResponse;
import com.acme.billing.v1.RetryInvoiceWorkflowRequest;
import com.acme.billing.v1.RetryInvoiceWorkflowResponse;
import com.acme.billing.v1.SignalInvoiceWorkflowRequest;
import com.acme.billing.v1.SignalInvoiceWorkflowResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.StartInvoiceWorkflowRequest;
import com.acme.billing.v1.StartInvoiceWorkflowResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentCalculationServiceGrpc;
import com.acme.itsm.v1.RecalculateIncidentsRequest;
import com.acme.itsm.v1.RecalculateIncidentsResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.surveys.v1.BrowseSurveysRequest;
import com.acme.surveys.v1.BrowseSurveysResponse;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes an invoice from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceWorkflowApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking;

    private final CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceFutureStub billingCreditNoteApprovalServiceFuture;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final IncidentCalculationServiceGrpc.IncidentCalculationServiceBlockingStub itsmIncidentCalculationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking;

    public InvoiceWorkflowApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking,
            CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceFutureStub billingCreditNoteApprovalServiceFuture,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            IncidentCalculationServiceGrpc.IncidentCalculationServiceBlockingStub itsmIncidentCalculationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingCreditNoteApprovalServiceBlocking = billingCreditNoteApprovalServiceBlocking;
        this.billingCreditNoteApprovalServiceFuture = billingCreditNoteApprovalServiceFuture;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingInvoiceApprovalServiceBlocking = billingInvoiceApprovalServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersConsentRecordValidationServiceFuture = customersConsentRecordValidationServiceFuture;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.itsmIncidentCalculationServiceBlocking = itsmIncidentCalculationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.surveysSurveyQueryServiceBlocking = surveysSurveyQueryServiceBlocking;
    }

    /**
     * Starts an invoice.
     */
    public StartInvoiceWorkflowResponse startInvoiceWorkflow(StartInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = UpdateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoice().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<UpdateAccessPolicyRequest, UpdateAccessPolicyResponse>() {
            @Override
            public UpdateAccessPolicyResponse apply(UpdateAccessPolicyRequest input) {
                return authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAccessPolicy(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RecalculateIncidentsRequest.newBuilder()
                .setContext(request.getContext())
                .setIncident(Incident.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = itsmIncidentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateIncidents(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return StartInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAccessPolicy().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getIncident().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getIncident().getRequester().getEmail())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setTotal(request.getInvoice().getTotal())
                        .build())
                .build();
    }

    /**
     * Advances an invoice.
     */
    public AdvanceInvoiceWorkflowResponse advanceInvoiceWorkflow(AdvanceInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = RpcSupport.unpack(Any.pack(EscalateVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getInvoice().getAudit().getEtag())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(request.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()), EscalateVarianceRequest.class);
        var reply0 = budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateVariance(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return AdvanceInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(request.getInvoice().getCounterparty())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }

    /**
     * Cancels an invoice.
     */
    public CancelInvoiceWorkflowResponse cancelInvoiceWorkflow(CancelInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound0), rpcExecutor);

        var outbound1 = CheckCustomerSegmentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(CheckCustomerSegmentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerSegmentEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = EscalateBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateBillingAccountRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply1.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBillingAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return CancelInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setOutcome(CancelInvoiceWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys an invoice.
     */
    public RetryInvoiceWorkflowResponse retryInvoiceWorkflow(RetryInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound0), rpcExecutor);

        var outbound1 = CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getInvoice().getBillingAddress().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPaymentPromiseApproval(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return RetryInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(request.getInvoice().getCounterparty())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setOutcome(RetryInvoiceWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Pauses an invoice.
     */
    public PauseInvoiceWorkflowResponse pauseInvoiceWorkflow(PauseInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = GetInvoiceApprovalStatusRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getInvoiceId())
                .build();
        var reply0 = billingInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceApprovalStatus(outbound0);

        var outbound1 = ListPendingCreditNotesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingCreditNotesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getUpdatedAt())
                        .setRegion(reply0.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = billingCreditNoteApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingCreditNotes(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BrowseSurveysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSurveysRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getInvoice().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply0.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(reply0.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> surveysSurveyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSurveys(outbound2));

        var outbound3 = ScoreConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending3 = customersConsentRecordValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreConsentRecord(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return PauseInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getInvoice().getBillingAddress().getRegion())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .putAllMetadata(request.getInvoice().getMetadataMap())
                        .setNotes(reply0.getInvoice().getNotes())
                        .build())
                .setOutcome(PauseInvoiceWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes an invoice.
     */
    public ResumeInvoiceWorkflowResponse resumeInvoiceWorkflow(ResumeInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = SimulateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.CreditNoteCalculationService");
        var serviceName0 = "acme.billing.v1" + "." + "CreditNoteCalculationService";
        var methodName0 = "Sim" + "ulateCreditNote";
        var descriptor0 = MethodDescriptor.<SimulateCreditNoteRequest, SimulateCreditNoteResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SimulateCreditNoteRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SimulateCreditNoteResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getInvoice().getLinesList().stream()
                .map(element -> BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? BrowseLedgerAccountsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ExplainCreditNoteDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCreditNoteDecision(value));
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return ResumeInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getCreditNote().getBillingAddress().getRecipient())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .putAllMetadata(request.getInvoice().getMetadataMap())
                        .build())
                .setOutcome(ResumeInvoiceWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Returns an invoice.
     */
    public InspectInvoiceWorkflowResponse inspectInvoiceWorkflow(InspectInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "InspectInvoiceWorkflow");

        var outbound0 = ApproveCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setInvoiceId(request.getInvoiceId())
                                .build())
                        .build())
                .setDetails(ApproveCreditNoteRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveCreditNote(outbound0);
        return InspectInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }

    /**
     * Lists invoices a page at a time.
     */
    public ListInvoiceWorkflowsResponse listInvoiceWorkflows(ListInvoiceWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListInvoiceWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "ListInvoiceWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListInvoiceWorkflows", request.getPage().getPageToken());
        return ListInvoiceWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals an invoice.
     */
    public SignalInvoiceWorkflowResponse signalInvoiceWorkflow(SignalInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = AdvanceExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportWorkflowService");
        var serviceName0 = "acme.expenses.v1" + "." + "ExpenseReportWorkflowService";
        var methodName0 = "Adv" + "anceExpenseReportWorkflow";
        var descriptor0 = MethodDescriptor.<AdvanceExpenseReportWorkflowRequest, AdvanceExpenseReportWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(AdvanceExpenseReportWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(AdvanceExpenseReportWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = QuoteCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getExpenseReport().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setNetwork(switch (request.getInvoice().getSettlementCase()) {
                                    case CARD -> request.getInvoice().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCreditNote(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalInvoiceWorkflow", request.getInvoice().getInvoiceId());
        return SignalInvoiceWorkflowResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(reply1.getCreditNote().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .setIssuedAt(request.getInvoice().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
