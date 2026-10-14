package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.broadcasts.v1.SuggestBroadcastsRequest;
import com.acme.broadcasts.v1.SuggestBroadcastsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.ResolveUnderwritingConflictRequest;
import com.acme.insurance.v1.ResolveUnderwritingConflictResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.lending.v1.AdvanceLoanWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanWorkflowResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.InspectLoanApplicationWorkflowRequest;
import com.acme.lending.v1.InspectLoanApplicationWorkflowResponse;
import com.acme.lending.v1.InspectLoanWorkflowRequest;
import com.acme.lending.v1.InspectLoanWorkflowResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.SignalLoanWorkflowRequest;
import com.acme.lending.v1.SignalLoanWorkflowResponse;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.StartLoanWorkflowRequest;
import com.acme.lending.v1.StartLoanWorkflowResponse;
import com.acme.lending.v1.TerminateLoanRequest;
import com.acme.lending.v1.TerminateLoanResponse;
import com.acme.sponsorships.v1.FindSponsorsRequest;
import com.acme.sponsorships.v1.FindSponsorsResponse;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Runs the long-lived workflow that takes a loan from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LoanWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanWorkflowApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking;

    private final LoanWorkflowRequestMapper mapper;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceFutureStub venuesVenueQueryServiceFuture;

    public LoanWorkflowApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking,
            LoanWorkflowRequestMapper mapper,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceFutureStub venuesVenueQueryServiceFuture) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.invoicingInvoiceBatchServiceAsync = invoicingInvoiceBatchServiceAsync;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.lendingLoanLifecycleServiceBlocking = lendingLoanLifecycleServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.sponsorshipsSponsorQueryServiceBlocking = sponsorshipsSponsorQueryServiceBlocking;
        this.venuesVenueQueryServiceFuture = venuesVenueQueryServiceFuture;
    }

    /**
     * Starts a loan.
     */
    public StartLoanWorkflowResponse startLoanWorkflow(StartLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = SuggestBroadcastsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBroadcastsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getUpdatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(request.getLoan().getHolder().getLoanApplicationId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.broadcasts.v1.BroadcastQueryService");
        var serviceName0 = "acme.broadcasts.v1" + "." + "BroadcastQueryService";
        var methodName0 = "Sug" + "gestBroadcasts";
        var descriptor0 = MethodDescriptor.<SuggestBroadcastsRequest, SuggestBroadcastsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SuggestBroadcastsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SuggestBroadcastsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getLoan().getLimitsList().stream()
                .map(element -> NormalizePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setDiscount(PreferenceDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .setName(element.getName())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePreference(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? NormalizePreferenceResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateInvoiceBatchesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateInvoiceBatchesResponse.getDefaultInstance());
        var input2 = invoicingInvoiceBatchServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateInvoiceBatches(collector2);
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
        repository.recordDetail(request.getContext().getTenantId(), "StartLoanWorkflow", request.getLoan().getLoanId());
        return StartLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setHolder(request.getLoan().getHolder())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(StartLoanWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Advances a loan.
     */
    public AdvanceLoanWorkflowResponse advanceLoanWorkflow(AdvanceLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = mapper.advanceLoanWorkflowStage0(request);
        var reply0 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).inspectLoanApplicationWorkflow(outbound0);

        var outbound1 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getOpenedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound1);
        var reply1 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ScheduleRepaymentExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.lending.v1.RepaymentLifecycleService");
        var serviceName2 = "acme.lending.v1" + "." + "RepaymentLifecycleService";
        var methodName2 = "Sch" + "eduleRepaymentExpiry";
        var descriptor2 = MethodDescriptor.<ScheduleRepaymentExpiryRequest, ScheduleRepaymentExpiryResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleRepaymentExpiryRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleRepaymentExpiryResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = CreateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getRepayment().getLocation().getPostalCode())
                                .build())
                        .setCard(DocumentCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getLoanApplication().getSettlementCase()) {
                                    case CARD -> reply0.getLoanApplication().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDocument(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceLoanWorkflow", request.getLoan().getLoanId());
        return AdvanceLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getDocument().getAudit().getCreatedAt())
                                .build())
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setOutcome(AdvanceLoanWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a loan.
     */
    public CancelLoanWorkflowResponse cancelLoanWorkflow(CancelLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getCreatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(request.getLoan().getHolder().getLoanApplicationId())
                        .build())
                .build();
        var pending0 = venuesVenueQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelLoanWorkflow", request.getLoan().getLoanId());
        return CancelLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setBalance(request.getLoan().getBalance())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a loan.
     */
    public RetryLoanWorkflowResponse retryLoanWorkflow(RetryLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = BeanCopies.copy(NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setName(request.getLoan().getContact().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getLoan().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), NormalizeConsentRecordRequest.class);
        var reply0 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound0);

        var outbound1 = JsonCodec.roundTrip(RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RecalculateConsentRecordsRequest.class);
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConsentRecords(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryLoanWorkflow", request.getLoan().getLoanId());
        return RetryLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setBalance(request.getLoan().getBalance())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(RetryLoanWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses a loan.
     */
    public PauseLoanWorkflowResponse pauseLoanWorkflow(PauseLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = BeanCopies.copy(ResolveUnderwritingConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveUnderwritingConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoan().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build(), ResolveUnderwritingConflictRequest.class);
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveUnderwritingConflict(outbound0);

        var outbound1 = CountPremiumTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPremiumTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .build())
                .build();
        var adapter1 = new Function<CountPremiumTotalsRequest, CountPremiumTotalsResponse>() {
            @Override
            public CountPremiumTotalsResponse apply(CountPremiumTotalsRequest input) {
                return insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPremiumTotals(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = EscalateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateLoanRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getUnderwriting().getAssignee().getUserId())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLoan().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateLoan(value)).orElseThrow();

        var outbound3 = TerminateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(TerminateLoanRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPhone(request.getLoan().getContact().getPhone())
                                .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setLoanId(request.getLoan().getLoanId())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> lendingLoanLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).terminateLoan(value));
        log.atDebug().addKeyValue("auditValue", request.getLoan().getHolder().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseLoanWorkflow", request.getLoan().getHolder().getLoanApplicationId());
        return PauseLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setOutcome(PauseLoanWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public InspectLoanWorkflowResponse inspectLoanWorkflow(InspectLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "InspectLoanWorkflow");

        var outbound0 = RpcSupport.unpack(Any.pack(StartLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartLoanApplicationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build()), StartLoanApplicationWorkflowRequest.class);
        var reply0 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startLoanApplicationWorkflow(outbound0);
        return InspectLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Signals a loan.
     */
    public SignalLoanWorkflowResponse signalLoanWorkflow(SignalLoanWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalLoanWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = FindSponsorsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSponsorsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getCreatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(request.getLoan().getLoanId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(sponsorshipsSponsorQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSponsors(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getHolder().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalLoanWorkflow", request.getLoan().getHolder().getLoanApplicationId());
        return SignalLoanWorkflowResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
