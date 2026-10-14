package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.LookupSupplierContractRequest;
import com.acme.suppliers.v1.LookupSupplierContractResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.PeekSupplierContractRequest;
import com.acme.suppliers.v1.PeekSupplierContractResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
 * Answers read-only questions about supplier contracts, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class SupplierContractQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierContractQueryApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final SuppliersRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public SupplierContractQueryApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            SuppliersRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.events = events;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record SuggestSupplierContractsStage0Requested(SubscribeAuditEventUpdatesRequest request, CompletableFuture<SubscribeAuditEventUpdatesResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a supplier contract.
     */
    public LookupSupplierContractResponse lookupSupplierContract(LookupSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getSupplierContractId(), "LookupSupplierContract");

        var outbound0 = SimulateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication", outbound0);
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoanApplication(command0.payload());

        var outbound1 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input1 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupSupplierContractResponse.newBuilder()
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    public BrowseSupplierContractsResponse browseSupplierContracts(BrowseSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "BrowseSupplierContracts");

        var outbound0 = BeanCopies.copy(CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), CheckSupplierRatingEligibilityRequest.class);
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound0);

        var outbound1 = NormalizeSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getSupplierRating().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.suppliers.v1.SupplierRatingValidationService");
        var serviceName1 = "acme.suppliers.v1" + "." + "SupplierRatingValidationService";
        var methodName1 = "Nor" + "malizeSupplierRating";
        var descriptor1 = MethodDescriptor.<NormalizeSupplierRatingRequest, NormalizeSupplierRatingResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeSupplierRatingRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeSupplierRatingResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseSupplierContracts", request.getPage().getPageToken());
        return BrowseSupplierContractsResponse.newBuilder()
                .addSupplierContracts(SupplierContract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSupplierRating().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    public FindSupplierContractsResponse findSupplierContracts(FindSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "FindSupplierContracts");

        var outbound0 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindSupplierContracts", request.getPage().getPageToken());
        return FindSupplierContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts supplier contracts by status.
     */
    public CountSupplierContractsResponse countSupplierContracts(CountSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "CountSupplierContracts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountSupplierContracts", request.getPage().getPageToken());
        return CountSupplierContractsResponse.getDefaultInstance();
    }

    /**
     * Returns several supplier contracts at once.
     */
    public ResolveSupplierContractsResponse resolveSupplierContracts(ResolveSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "ResolveSupplierContracts");

        var outbound0 = CheckVenueEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckVenueEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.venues.v1.VenueValidationService");
        var selected0 = VenueValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.checkVenueEligibility(outbound0);
        return ResolveSupplierContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching supplier contract.
     */
    public ExportSupplierContractsResponse exportSupplierContracts(ExportSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "ExportSupplierContracts");

        var outbound0 = JsonCodec.roundTrip(FindSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSuppliersRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build(), FindSuppliersRequest.class);
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSuppliers(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(NormalizeSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), NormalizeSupplierRatingRequest.class);
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRating(outbound1);

        var outbound2 = WatchRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchRiskRegistersRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var stream2 = riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchRiskRegisters(outbound2);
        var reply2 = WatchRiskRegistersResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportSupplierContracts", request.getFilter().getQuery());
        return ExportSupplierContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getSupplierRating().getBillingAddress().getCountryCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a supplier contract.
     */
    public SummarizeSupplierContractsResponse summarizeSupplierContracts(SummarizeSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getSupplierContractId(), "SummarizeSupplierContracts");

        var outbound0 = JsonCodec.roundTrip(LintVenuesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build(), LintVenuesRequest.class);
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintVenues(outbound0);

        var outbound1 = RecalculateLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRegion(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setQuantity(reply0.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoanApplications(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeSupplierContracts", request.getSupplierContractId());
        return SummarizeSupplierContractsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    public SuggestSupplierContractsResponse suggestSupplierContracts(SuggestSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "SuggestSupplierContracts");

        var outbound0 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SubscribeAuditEventUpdatesResponse>();
        events.publishEvent(new SuggestSupplierContractsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestSupplierContracts", request.getPage().getPageToken());
        return SuggestSupplierContractsResponse.newBuilder()
                .addSupplierContracts(SupplierContract.newBuilder()
                        .addLines(SupplierContractLine.newBuilder()
                                .putAllAttributes(reply0.getAuditEvent().getAttributesMap())
                                .build())
                        .setNumber(reply0.getAuditEvent().getSubject().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns a supplier contract.
     */
    public PeekSupplierContractResponse peekSupplierContract(PeekSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getSupplierContractId(), "PeekSupplierContract");

        var outbound0 = ExportSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSuppliersRequest.Filter.newBuilder()
                        .setOwnerId(request.getSupplierContractId())
                        .build())
                .build();
        var stream0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSuppliers(outbound0);
        var reply0 = ExportSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        return PeekSupplierContractResponse.newBuilder()
                .setSupplierContract(SupplierContract.newBuilder()
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }
}
