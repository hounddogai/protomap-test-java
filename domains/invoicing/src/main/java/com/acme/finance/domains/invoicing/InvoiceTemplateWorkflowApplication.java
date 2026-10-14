package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CloneAuditEventRequest;
import com.acme.audit.v1.CloneAuditEventResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.CloneCustomerRequest;
import com.acme.customers.v1.CloneCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerPassport;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.GetCustomerRatesRequest;
import com.acme.customers.v1.GetCustomerRatesResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesRequest;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesResponse;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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
 * Runs the long-lived workflow that takes an invoice template from start to finish. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class InvoiceTemplateWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateWorkflowApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceFutureStub customersCustomerServiceFuture;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking;

    private final InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public InvoiceTemplateWorkflowApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceFutureStub customersCustomerServiceFuture,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking,
            InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersCustomerServiceFuture = customersCustomerServiceFuture;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.invoicingEInvoiceReportingServiceBlocking = invoicingEInvoiceReportingServiceBlocking;
        this.invoicingInvoiceTemplateQueryServiceBlocking = invoicingInvoiceTemplateQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Starts an invoice template.
     */
    public StartInvoiceTemplateWorkflowResponse startInvoiceTemplateWorkflow(StartInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = CloneCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getInvoiceTemplate().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceTemplate().getCounterparty().getEmail())
                                .build())
                        .setPassport(CustomerPassport.newBuilder()
                                .setNumber(request.getInvoiceTemplate().getNumber())
                                .build())
                        .build())
                .build();
        var pending0 = customersCustomerServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCustomer(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(switch (request.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> request.getInvoiceTemplate().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(outbound1);

        List<CheckHouseholdEligibilityResponse> results2 = request.getInvoiceTemplate().getLinesList().stream()
                .map(element -> CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .addChildren(Household.newBuilder()
                                                .putAllAttributes(element.getAttributesMap())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(reply1.getAuditTrail().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckHouseholdEligibilityResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return StartInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getCustomer().getPrimaryAddress().getLinesList())
                                .setPostalCode(reply1.getAuditTrail().getBillingAddress().getPostalCode())
                                .build())
                        .setDueAt(reply1.getAuditTrail().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .addLines(InvoiceTemplateLine.newBuilder()
                                .putAllAttributes(reply0.getCustomer().getAttributesMap())
                                .build())
                        .putAllMetadata(request.getInvoiceTemplate().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Advances an invoice template.
     */
    public AdvanceInvoiceTemplateWorkflowResponse advanceInvoiceTemplateWorkflow(AdvanceInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var builder0 = ValidateVenueRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setVenue(Venue.newBuilder()
                .setAddress(PostalAddress.newBuilder()
                        .setPostalCode(request.getInvoiceTemplate().getBillingAddress().getPostalCode())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(outbound0);

        var results1 = new ArrayList<BrowseInvoiceTemplatesResponse>();
        for (var element : request.getInvoiceTemplate().getLinesList()) {
            BrowseInvoiceTemplatesRequest itemRequest = BrowseInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseInvoiceTemplatesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
            results1.add(invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseInvoiceTemplates(itemRequest));
        }
        var reply1 = results1.isEmpty() ? BrowseInvoiceTemplatesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return AdvanceInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getVenue().getAddress().getPostalCode())
                                .build())
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Retrys an invoice template.
     */
    public RetryInvoiceTemplateWorkflowResponse retryInvoiceTemplateWorkflow(RetryInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = SearchAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAuditEventsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        final class Stage0Adapter {
            SearchAuditEventsResponse fetch(SearchAuditEventsRequest input) {
                return auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchAuditEvents(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return RetryInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(request.getInvoiceTemplate().getBillingAddress())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses an invoice template.
     */
    public PauseInvoiceTemplateWorkflowResponse pauseInvoiceTemplateWorkflow(PauseInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = JsonCodec.roundTrip(ResolveInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveInvoiceTemplatesRequest.class);
        var reply0 = invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveInvoiceTemplates(outbound0);

        var outbound1 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getIssuedAt())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        final class Stage1Adapter {
            FindHouseholdsResponse fetch(FindHouseholdsRequest input) {
                return customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findHouseholds(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return PauseInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(request.getInvoiceTemplate().getCounterparty())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes an invoice template.
     */
    public ResumeInvoiceTemplateWorkflowResponse resumeInvoiceTemplateWorkflow(ResumeInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceTemplate().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound0), rpcExecutor);

        var outbound1 = GetCustomerRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerId(request.getInvoiceTemplate().getCounterparty().getCustomerId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCustomerRates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ResumeInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCustomerSegment().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setStatus(request.getInvoiceTemplate().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an invoice template.
     */
    public InspectInvoiceTemplateWorkflowResponse inspectInvoiceTemplateWorkflow(InspectInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "InspectInvoiceTemplateWorkflow");

        var outbound0 = BeanCopies.copy(CountEInvoiceTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountEInvoiceTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getInvoiceTemplateId())
                        .build())
                .build(), CountEInvoiceTotalsRequest.class);
        var reply0 = invoicingEInvoiceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countEInvoiceTotals(outbound0);

        var outbound1 = ExportInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportInvoiceTemplatesRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var stream1 = invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportInvoiceTemplates(outbound1);
        var reply1 = ExportInvoiceTemplatesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        return InspectInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists invoice templates a page at a time.
     */
    public ListInvoiceTemplateWorkflowsResponse listInvoiceTemplateWorkflows(ListInvoiceTemplateWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListInvoiceTemplateWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "ListInvoiceTemplateWorkflows");

        var outbound0 = CloneAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getFilter().getLabelsMap())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setSource(request.getFilter().getOwnerId())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneAuditEvent(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListInvoiceTemplateWorkflows", request.getPage().getPageToken());
        return ListInvoiceTemplateWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals an invoice template.
     */
    public SignalInvoiceTemplateWorkflowResponse signalInvoiceTemplateWorkflow(SignalInvoiceTemplateWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalInvoiceTemplateWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceTemplate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditEventValidationService");
        var selected0 = AuditEventValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.normalizeAuditEvent(outbound0);

        var outbound1 = CountPremiumTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPremiumTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(reply0.getAuditEvent().getSubject().getAuditTrailId())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.PremiumReportingService/CountPremiumTotals", outbound1);
        var reply1 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPremiumTotals(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalInvoiceTemplateWorkflow", request.getInvoiceTemplate().getInvoiceTemplateId());
        return SignalInvoiceTemplateWorkflowResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(request.getInvoiceTemplate().getCounterparty())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(SignalInvoiceTemplateWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
