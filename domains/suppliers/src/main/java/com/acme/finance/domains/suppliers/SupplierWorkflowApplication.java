package com.acme.finance.domains.suppliers;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.BatchCreateCorporateEventsRequest;
import com.acme.corporateevents.v1.BatchCreateCorporateEventsResponse;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryRequest;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingLifecycleServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowResponse;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.InspectSupplierWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierWorkflowResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PauseSupplierWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierWorkflowResponse;
import com.acme.suppliers.v1.PeekSupplierRequest;
import com.acme.suppliers.v1.PeekSupplierResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.ResumeSupplierWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierWorkflowResponse;
import com.acme.suppliers.v1.SuggestSuppliersRequest;
import com.acme.suppliers.v1.SuggestSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierPreferences;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.acme.timekeeping.v1.CreateTimesheetRequest;
import com.acme.timekeeping.v1.CreateTimesheetResponse;
import com.acme.timekeeping.v1.Timesheet;
import com.acme.timekeeping.v1.TimesheetServiceGrpc;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a supplier from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierWorkflowApplication.class);

    private final ChannelRegistry channels;

    private final CorporateEventServiceGrpc.CorporateEventServiceStub corporateeventsCorporateEventServiceAsync;

    private final UnderwritingLifecycleServiceGrpc.UnderwritingLifecycleServiceBlockingStub insuranceUnderwritingLifecycleServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final SupplierWorkflowRequestMapper mapper;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final TimesheetServiceGrpc.TimesheetServiceBlockingStub timekeepingTimesheetServiceBlocking;

    public SupplierWorkflowApplication(
            ChannelRegistry channels,
            CorporateEventServiceGrpc.CorporateEventServiceStub corporateeventsCorporateEventServiceAsync,
            UnderwritingLifecycleServiceGrpc.UnderwritingLifecycleServiceBlockingStub insuranceUnderwritingLifecycleServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            SupplierWorkflowRequestMapper mapper,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            TimesheetServiceGrpc.TimesheetServiceBlockingStub timekeepingTimesheetServiceBlocking) {
        this.channels = channels;
        this.corporateeventsCorporateEventServiceAsync = corporateeventsCorporateEventServiceAsync;
        this.insuranceUnderwritingLifecycleServiceBlocking = insuranceUnderwritingLifecycleServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.timekeepingTimesheetServiceBlocking = timekeepingTimesheetServiceBlocking;
    }

    /**
     * Starts a supplier.
     */
    public StartSupplierWorkflowResponse startSupplierWorkflow(StartSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = SummarizeSupplierContractsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSupplierContractsRequest.Details.newBuilder()
                        .putAllFilters(request.getSupplier().getAttributesMap())
                        .setPeriod(request.getSupplier().getPreferences().getQuietHours())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<SummarizeSupplierContractsRequest, SummarizeSupplierContractsResponse> port0 = RpcPort.adapt(suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeSupplierContracts);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartSupplierWorkflow", request.getSupplier().getSupplierId());
        return StartSupplierWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSupplier().getName())
                                .build())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Advances a supplier.
     */
    public AdvanceSupplierWorkflowResponse advanceSupplierWorkflow(AdvanceSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var builder0 = BrowseSuppliersRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseSuppliersRequest.Filter.newBuilder()
                .setCreatedAfter(request.getSupplier().getPassport().getExpiresAt())
                .setOwnerId(request.getSupplier().getSupplierId())
                .setRegion(request.getSupplier().getPrimaryAddress().getRegion())
                .build());
        var outbound0 = builder0.build();
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSuppliers(outbound0);

        var outbound1 = (PreviewSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRiskRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplier().getContact().getEmail())
                                .setName(request.getSupplier().getContact().getName())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSupplier().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSupplier().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRisk(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getContact().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceSupplierWorkflow", request.getSupplier().getContact().getPhone());
        return AdvanceSupplierWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setName(request.getSupplier().getName())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Cancels a supplier.
     */
    public CancelSupplierWorkflowResponse cancelSupplierWorkflow(CancelSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = WatchSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getSupplier().getRelationshipManager().getEmail())
                        .build())
                .build();
        var stream0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchSuppliers(outbound0);
        var reply0 = WatchSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BatchCreateCorporateEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(CorporateEvent.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getSupplier().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplier().getPrimaryAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateCorporateEventsResponse.getDefaultInstance());
        var input1 = corporateeventsCorporateEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCorporateEvents(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelSupplierWorkflow", request.getSupplier().getSupplierId());
        return CancelSupplierWorkflowResponse.newBuilder()
                .setSupplier(Supplier.newBuilder()
                        .setPrimaryAddress(request.getSupplier().getPrimaryAddress())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Retrys a supplier.
     */
    public RetrySupplierWorkflowResponse retrySupplierWorkflow(RetrySupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetrySupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = LintVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintVenuesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplier().getContact().getEmail())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplier().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.venues.v1.VenueValidationService");
        var serviceName0 = "acme.venues.v1" + "." + "VenueValidationService";
        var methodName0 = "Lin" + "tVenues";
        var descriptor0 = MethodDescriptor.<LintVenuesRequest, LintVenuesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(LintVenuesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(LintVenuesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = PeekSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierId(request.getSupplier().getSupplierId())
                .build();
        RpcPort<PeekSupplierRequest, PeekSupplierResponse> port1 = RpcPort.adapt(suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::peekSupplier);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = LookupSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierId(request.getSupplierId())
                .build();
        RpcPort<LookupSupplierRequest, LookupSupplierResponse> port2 = RpcPort.adapt(suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lookupSupplier);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = ListPendingSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getSupplier().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getComponentId())
                        .setRegion(reply2.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingSupplierContracts(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getContact().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetrySupplierWorkflow", request.getSupplier().getContact().getPhone());
        return RetrySupplierWorkflowResponse.newBuilder()
                .setOutcome(RetrySupplierWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getVenue().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSupplier().getName())
                                .build())
                        .setName(request.getSupplier().getName())
                        .setPreferences(SupplierPreferences.newBuilder()
                                .setLanguage(reply2.getSupplier().getPreferences().getLanguage())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getSupplier().getPrimaryAddress().getCountryCode())
                                .build())
                        .setSupplierId(reply1.getSupplier().getSupplierId())
                        .build())
                .build();
    }

    /**
     * Pauses a supplier.
     */
    public PauseSupplierWorkflowResponse pauseSupplierWorkflow(PauseSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = (RecallSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallSupplierContractRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getSupplier().getRelationshipManager().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallSupplierContract(outbound0);

        var outbound1 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSupplierContract().getAudit().getUpdatedAt())
                        .setRegion(request.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseSupplierWorkflow", request.getSupplier().getSupplierId());
        return PauseSupplierWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setContact(request.getSupplier().getContact())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getSupplierContract().getBillingAddress().getCountryCode())
                                .build())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Resumes a supplier.
     */
    public ResumeSupplierWorkflowResponse resumeSupplierWorkflow(ResumeSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = RpcSupport.unpack(Any.pack(PeekSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierId(request.getSupplier().getSupplierId())
                .build()), PeekSupplierRequest.class);
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekSupplier(outbound0);

        var outbound1 = UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getSupplier().getRelationshipManager().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<UpdateSearchIndexRequest, UpdateSearchIndexResponse> port1 = RpcPort.adapt(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::updateSearchIndex);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = EscalateSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateSupplierContractRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply0.getSupplier().getContact().getEmail())
                                .build())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getSupplier().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getSupplier().getPrimaryAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateSupplierContract(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getContact().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeSupplierWorkflow", request.getSupplier().getContact().getEmail());
        return ResumeSupplierWorkflowResponse.newBuilder()
                .setOutcome(ResumeSupplierWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getSupplier().getAudit().getUpdatedAt())
                                .build())
                        .setRelationshipManager(request.getSupplier().getRelationshipManager())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier.
     */
    public InspectSupplierWorkflowResponse inspectSupplierWorkflow(InspectSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "InspectSupplierWorkflow");

        var outbound0 = SummarizeSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeSupplierContractsRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        RpcPort<SummarizeSupplierContractsRequest, SummarizeSupplierContractsResponse> port0 = RpcPort.adapt(suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeSupplierContracts);
        var reply0 = port0.exchange(outbound0);
        return InspectSupplierWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Lists suppliers a page at a time.
     */
    public ListSupplierWorkflowsResponse listSupplierWorkflows(ListSupplierWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListSupplierWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "ListSupplierWorkflows");

        var outbound0 = SuggestSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setRegion(request.getFilter().getRegion())
                        .addAllStatuses(request.getFilter().getStatusesList())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSuppliers(outbound0), rpcExecutor);

        var outbound1 = CreateTimesheetRequest.newBuilder()
                .setContext(request.getContext())
                .setTimesheet(Timesheet.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> timekeepingTimesheetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createTimesheet(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListSupplierWorkflows", request.getPage().getPageToken());
        return ListSupplierWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a supplier.
     */
    public SignalSupplierWorkflowResponse signalSupplierWorkflow(SignalSupplierWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalSupplierWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = SuggestSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplier().getAudit().getUpdatedAt())
                        .setOwnerId(request.getSupplier().getSupplierId())
                        .setRegion(request.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSuppliers(value)).orElseThrow();

        var outbound1 = mapper.signalSupplierWorkflowStage1(request);
        var reply1 = insuranceUnderwritingLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleUnderwritingExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getContact().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalSupplierWorkflow", request.getSupplier().getContact().getPhone());
        return SignalSupplierWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getUnderwriting().getAudit().getCreatedBy())
                                .build())
                        .setName(request.getSupplier().getName())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(reply1.getUnderwriting().getAssignee().getUserId())
                                .build())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }
}
