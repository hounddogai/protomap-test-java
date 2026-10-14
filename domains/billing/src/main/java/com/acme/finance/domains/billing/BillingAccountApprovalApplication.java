package com.acme.finance.domains.billing;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.billing.v1.ApproveBillingAccountRequest;
import com.acme.billing.v1.ApproveBillingAccountResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CommentBillingAccountApprovalRequest;
import com.acme.billing.v1.CommentBillingAccountApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.DelegateBillingAccountApprovalRequest;
import com.acme.billing.v1.DelegateBillingAccountApprovalResponse;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.ListPendingBillingAccountsResponse;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.RecallBillingAccountRequest;
import com.acme.billing.v1.RecallBillingAccountResponse;
import com.acme.billing.v1.RejectBillingAccountRequest;
import com.acme.billing.v1.RejectBillingAccountResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.StreamDunningMetricsRequest;
import com.acme.billing.v1.StreamDunningMetricsResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.SearchCustomersRequest;
import com.acme.customers.v1.SearchCustomersResponse;
import com.acme.documents.v1.BrowseDocumentsRequest;
import com.acme.documents.v1.BrowseDocumentsResponse;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.GetUserRequest;
import com.acme.identity.v1.GetUserResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.personalization.v1.ListProfilesRequest;
import com.acme.personalization.v1.ListProfilesResponse;
import com.acme.personalization.v1.ProfileServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Routes billing accounts through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BillingAccountApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(BillingAccountApprovalApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceBlockingStub billingCreditNoteLifecycleServiceBlocking;

    private final DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceFutureStub documentsDocumentValidationServiceFuture;

    private final ApplicationEventPublisher events;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public BillingAccountApprovalApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceBlockingStub billingCreditNoteLifecycleServiceBlocking,
            DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking,
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceFutureStub documentsDocumentValidationServiceFuture,
            ApplicationEventPublisher events,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingCreditNoteLifecycleServiceBlocking = billingCreditNoteLifecycleServiceBlocking;
        this.billingDunningReportingServiceBlocking = billingDunningReportingServiceBlocking;
        this.channels = channels;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.documentsDocumentValidationServiceFuture = documentsDocumentValidationServiceFuture;
        this.events = events;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.personalizationProfileServiceBlocking = personalizationProfileServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ApproveBillingAccountStage1Requested(SearchCustomersRequest request, CompletableFuture<SearchCustomersResponse> completion, io.grpc.Context context) {}

    public record ListPendingBillingAccountsStage1Requested(BrowseDocumentsRequest request, CompletableFuture<BrowseDocumentsResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a billing account.
     */
    public SubmitBillingAccountResponse submitBillingAccount(SubmitBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = CheckDocumentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckDocumentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getBillingAccount().getContact().getPhone())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setCard(DocumentCardSettlement.newBuilder()
                                .setLast4(request.getBillingAccount().getCard().getLast4())
                                .setToken(request.getBillingAccount().getCard().getToken())
                                .build())
                        .build())
                .build();
        var pending0 = documentsDocumentValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkDocumentEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitBillingAccount", request.getBillingAccount().getBillingAccountId());
        return SubmitBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(request.getBillingAccount().getContact())
                        .build())
                .setOutcome(SubmitBillingAccountResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves a billing account.
     */
    public ApproveBillingAccountResponse approveBillingAccount(ApproveBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = AnnotateAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setToken(switch (request.getBillingAccount().getFundingCase()) {
                                    case CARD -> request.getBillingAccount().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setNumber(request.getBillingAccount().getHolder().getNumber())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditTrailVersion(outbound0);

        var outbound1 = SearchCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAuditTrail().getAudit().getCreatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<SearchCustomersResponse>();
        events.publishEvent(new ApproveBillingAccountStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveBillingAccount", request.getBillingAccount().getBillingAccountId());
        return ApproveBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setStatus(request.getBillingAccount().getStatus())
                        .build())
                .setOutcome(ApproveBillingAccountResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a billing account.
     */
    public RejectBillingAccountResponse rejectBillingAccount(RejectBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = PreviewBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBillingAccount().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<PreviewBillingAccountRequest, PreviewBillingAccountResponse> operation0 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewBillingAccount;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .build()), BrowseCustomersRequest.class);
        var reply1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectBillingAccount", request.getBillingAccount().getBillingAccountId());
        return RejectBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getBillingAccount().getContact().getPhone())
                                .setPreferredChannel(reply0.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setOutcome(RejectBillingAccountResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Escalates a billing account.
     */
    public EscalateBillingAccountResponse escalateBillingAccount(EscalateBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = StreamDunningMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .setName(request.getBillingAccount().getContact().getName())
                        .setPhone(request.getBillingAccount().getContact().getPhone())
                        .build())
                .build();
        var stream0 = billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDunningMetrics(outbound0);
        var reply0 = StreamDunningMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CountCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBillingAccount().getAudit().getCreatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.CustomerQueryService");
        var selected1 = CustomerQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.countCustomers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateBillingAccount", request.getBillingAccount().getBillingAccountId());
        return EscalateBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .setOutcome(EscalateBillingAccountResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public ListPendingBillingAccountsResponse listPendingBillingAccounts(ListPendingBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "ListPendingBillingAccounts");

        var outbound0 = GetUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getContext().getActor().getUserId())
                .build();
        var command0 = new DispatchCommand<>("acme.identity.v1.UserService/GetUser", outbound0);
        var reply0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getUser(command0.payload());

        var outbound1 = BrowseDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseDocumentsRequest.Filter.newBuilder()
                        .setRegion(reply0.getUser().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<BrowseDocumentsResponse>();
        events.publishEvent(new ListPendingBillingAccountsStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingBillingAccounts", request.getPage().getPageToken());
        return ListPendingBillingAccountsResponse.newBuilder()
                .addBillingAccounts(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUser().getAudit().getEtag())
                                .build())
                        .build())
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply1.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a billing account.
     */
    public DelegateBillingAccountApprovalResponse delegateBillingAccountApproval(DelegateBillingAccountApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateBillingAccountApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ListProfilesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListProfilesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBillingAccount().getOpenedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setOwnerId(request.getBillingAccount().getHolder().getInvoiceId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> personalizationProfileServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listProfiles(outbound0), rpcExecutor);

        var outbound1 = ScoreBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBillingAccount().getAudit().getCreatedAt())
                                .setEtag(request.getBillingAccount().getAudit().getEtag())
                                .build())
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setAccountHolder(request.getBillingAccount().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBillingAccount(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateBillingAccountApproval", request.getBillingAccount().getBillingAccountId());
        return DelegateBillingAccountApprovalResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getBillingAccount().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setStatus(request.getBillingAccount().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalls a billing account.
     */
    public RecallBillingAccountResponse recallBillingAccount(RecallBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ScheduleCreditNoteExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setLast4(request.getBillingAccount().getCard().getLast4())
                                .build())
                        .build())
                .setDetails(ScheduleCreditNoteExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getBillingAccount().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> billingCreditNoteLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCreditNoteExpiry(value)).orElseThrow();

        var outbound1 = ListAuditTrailVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBillingAccount().getAudit().getCreatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setRegion(reply0.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailVersions(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallBillingAccount", request.getBillingAccount().getBillingAccountId());
        return RecallBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(request.getBillingAccount().getContact())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(reply0.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .setOutcome(RecallBillingAccountResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Comments a billing account.
     */
    public CommentBillingAccountApprovalResponse commentBillingAccountApproval(CommentBillingAccountApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentBillingAccountApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ExplainCreditNoteCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(ExplainCreditNoteCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setName(request.getBillingAccount().getContact().getName())
                                .setPreferredChannel(request.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.CreditNoteCalculationService");
        var serviceName0 = "acme.billing.v1" + "." + "CreditNoteCalculationService";
        var methodName0 = "Exp" + "lainCreditNoteCalculation";
        var descriptor0 = MethodDescriptor.<ExplainCreditNoteCalculationRequest, ExplainCreditNoteCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainCreditNoteCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainCreditNoteCalculationResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = NotifyAuditTrailWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditTrailWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getBillingAccount().getContact().getEmail())
                                .setName(request.getBillingAccount().getContact().getName())
                                .setPhone(request.getBillingAccount().getContact().getPhone())
                                .setPreferredChannel(request.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditTrailWatchers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentBillingAccountApproval", request.getBillingAccount().getBillingAccountId());
        return CommentBillingAccountApprovalResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setBillingAccountId(request.getBillingAccountId())
                        .build())
                .setOutcome(CommentBillingAccountApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
