package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.ValidateCustomerRequest;
import com.acme.customers.v1.ValidateCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.GetUserRequest;
import com.acme.identity.v1.GetUserResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.DeleteRepaymentReportRequest;
import com.acme.lending.v1.DeleteRepaymentReportResponse;
import com.acme.lending.v1.FetchRepaymentReportRequest;
import com.acme.lending.v1.FetchRepaymentReportResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentReportRequest;
import com.acme.lending.v1.ScheduleRepaymentReportResponse;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about repayments. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class RepaymentReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(RepaymentReportingApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceFutureStub lendingLoanApplicationQueryServiceFuture;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final LendingRepository repository;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public RepaymentReportingApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceFutureStub lendingLoanApplicationQueryServiceFuture,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            LendingRepository repository,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.lendingLoanApplicationQueryServiceFuture = lendingLoanApplicationQueryServiceFuture;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.repository = repository;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Generates a repayment.
     */
    public GenerateRepaymentReportResponse generateRepaymentReport(GenerateRepaymentReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateRepaymentReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "GenerateRepaymentReport");

        var outbound0 = NotifyPostingBatchWatchersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NotifyPostingBatchWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getDetails().getFiltersMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.PostingBatchNotificationService");
        var serviceName0 = "acme.ledger.v1" + "." + "PostingBatchNotificationService";
        var methodName0 = "Not" + "ifyPostingBatchWatchers";
        var descriptor0 = MethodDescriptor.<NotifyPostingBatchWatchersRequest, NotifyPostingBatchWatchersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(NotifyPostingBatchWatchersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NotifyPostingBatchWatchersResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = JsonCodec.roundTrip(SuggestLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPostingBatch().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getPostingBatch().getMetadataMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build(), SuggestLoanApplicationsRequest.class);
        var reply1 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestLoanApplications(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateRepaymentReport", request.getRepaymentId());
        return GenerateRepaymentReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a repayment.
     */
    public FetchRepaymentReportResponse fetchRepaymentReport(FetchRepaymentReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchRepaymentReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "FetchRepaymentReport");

        var outbound0 = ListPendingPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getRepaymentId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPaymentPromises(value)).orElseThrow();
        return FetchRepaymentReportResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a repayment.
     */
    public ScheduleRepaymentReportResponse scheduleRepaymentReport(ScheduleRepaymentReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleRepaymentReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "ScheduleRepaymentReport");

        var outbound0 = ValidateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateCustomerRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCustomer(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = PreviewVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                                .setPhone(reply0.getCustomer().getContact().getPhone())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .setEmail(reply0.getCustomer().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.venues.v1.VenueValidationService/PreviewVenue", outbound1);
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVenue(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleRepaymentReport", request.getRepaymentId());
        return ScheduleRepaymentReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of repayments as they happen.
     */
    public StreamRepaymentMetricsResponse streamRepaymentMetrics(StreamRepaymentMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamRepaymentMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getContext().getTraceId(), "StreamRepaymentMetrics");

        var outbound0 = GetUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getContext().getActor().getUserId())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getUser(value));

        var outbound1 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(outbound1));

        var outbound2 = ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUser().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamRepaymentMetrics", request.getFilter().getQuery());
        return StreamRepaymentMetricsResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUser().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(reply0.getUser().getContact().getName())
                                        .setPhone(reply2.getCustomer().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Shares a repayment.
     */
    public ShareRepaymentReportResponse shareRepaymentReport(ShareRepaymentReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareRepaymentReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "ShareRepaymentReport");

        var outbound0 = QuoteRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getRecipient().getName())
                                        .setPhone(request.getDetails().getRecipient().getPhone())
                                        .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            QuoteRepaymentResponse fetch(QuoteRepaymentRequest input) {
                return lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteRepayment(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareRepaymentReport", request.getDetails().getTemplateId());
        return ShareRepaymentReportResponse.newBuilder()
                .setOutcome(ShareRepaymentReportResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRepayment().getAudit().getCreatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }

    /**
     * Counts repayments by status.
     */
    public CountRepaymentTotalsResponse countRepaymentTotals(CountRepaymentTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountRepaymentTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getContext().getTraceId(), "CountRepaymentTotals");

        var outbound0 = BrowseLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoanApplicationsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var pending0 = lendingLoanApplicationQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browseLoanApplications(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountRepaymentTotals", request.getPage().getPageToken());
        return CountRepaymentTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a repayment.
     */
    public DeleteRepaymentReportResponse deleteRepaymentReport(DeleteRepaymentReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteRepaymentReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "DeleteRepaymentReport");

        var outbound0 = ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareFiscalPeriodExport(value));

        var outbound1 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<SubscribeAuditEventUpdatesRequest, SubscribeAuditEventUpdatesResponse>() {
            @Override
            public SubscribeAuditEventUpdatesResponse apply(SubscribeAuditEventUpdatesRequest input) {
                return auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteRepaymentReport", request.getRepaymentId());
        return DeleteRepaymentReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
