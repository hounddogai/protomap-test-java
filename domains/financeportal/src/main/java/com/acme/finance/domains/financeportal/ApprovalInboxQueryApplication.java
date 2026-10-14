package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SubmitCustomerSegmentRequest;
import com.acme.customers.v1.SubmitCustomerSegmentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.BrowseApprovalInboxesRequest;
import com.acme.financeportal.v1.BrowseApprovalInboxesResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistLine;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.ExportApprovalInboxesRequest;
import com.acme.financeportal.v1.ExportApprovalInboxesResponse;
import com.acme.financeportal.v1.FinanceWorkspaceQueryServiceGrpc;
import com.acme.financeportal.v1.FindApprovalInboxesRequest;
import com.acme.financeportal.v1.FindApprovalInboxesResponse;
import com.acme.financeportal.v1.LookupApprovalInboxRequest;
import com.acme.financeportal.v1.LookupApprovalInboxResponse;
import com.acme.financeportal.v1.PeekApprovalInboxRequest;
import com.acme.financeportal.v1.PeekApprovalInboxResponse;
import com.acme.financeportal.v1.ResolveApprovalInboxesRequest;
import com.acme.financeportal.v1.ResolveApprovalInboxesResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeApprovalInboxesRequest;
import com.acme.financeportal.v1.SummarizeApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.webbff.v1.PageCompositionQueryServiceGrpc;
import com.acme.webbff.v1.ResolvePageCompositionsRequest;
import com.acme.webbff.v1.ResolvePageCompositionsResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about approval inboxes, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ApprovalInboxQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ApprovalInboxQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking;

    private final FinanceWorkspaceQueryServiceGrpc.FinanceWorkspaceQueryServiceBlockingStub financeportalFinanceWorkspaceQueryServiceBlocking;

    private final ApprovalInboxQueryRequestMapper mapper;

    private final FinanceportalRepository repository;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public ApprovalInboxQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            ChannelRegistry channels,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            ApplicationEventPublisher events,
            CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking,
            FinanceWorkspaceQueryServiceGrpc.FinanceWorkspaceQueryServiceBlockingStub financeportalFinanceWorkspaceQueryServiceBlocking,
            ApprovalInboxQueryRequestMapper mapper,
            FinanceportalRepository repository,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.channels = channels;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.events = events;
        this.financeportalCloseChecklistWorkflowServiceBlocking = financeportalCloseChecklistWorkflowServiceBlocking;
        this.financeportalFinanceWorkspaceQueryServiceBlocking = financeportalFinanceWorkspaceQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record PeekApprovalInboxStage0Requested(GenerateTaxWithholdingReportRequest request, CompletableFuture<GenerateTaxWithholdingReportResponse> completion, io.grpc.Context context) {}

    /**
     * Returns an approval inbox.
     */
    public LookupApprovalInboxResponse lookupApprovalInbox(LookupApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "LookupApprovalInbox");
        return LookupApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public BrowseApprovalInboxesResponse browseApprovalInboxes(BrowseApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "BrowseApprovalInboxes");

        var builder0 = SummarizeFinanceWorkspacesRequest.newBuilder();
        builder0.putAllAnnotations(request.getContext().getBaggageMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(SummarizeFinanceWorkspacesRequest.Details.newBuilder()
                .putAllFilters(request.getFilter().getLabelsMap())
                .build());
        var outbound0 = builder0.build();
        var reply0 = financeportalFinanceWorkspaceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeFinanceWorkspaces(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseApprovalInboxes", request.getPage().getPageToken());
        return BrowseApprovalInboxesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public FindApprovalInboxesResponse findApprovalInboxes(FindApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "FindApprovalInboxes");

        var builder0 = SimulateCustomerSegmentRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setCustomerSegment(CustomerSegment.newBuilder()
                .setBlock(CustomerSegmentBlockAction.newBuilder()
                        .setNotifyEmail(request.getContext().getActor().getEmail())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        builder0.setDetails(SimulateCustomerSegmentRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindApprovalInboxes", request.getPage().getPageToken());
        return FindApprovalInboxesResponse.newBuilder()
                .addApprovalInboxes(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCustomerSegment().getAudit().getUpdatedAt())
                                .build())
                        .addLines(ApprovalInboxLine.newBuilder()
                                .setDescription(reply0.getCustomerSegment().getDescription())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts approval inboxes by status.
     */
    public CountApprovalInboxesResponse countApprovalInboxes(CountApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "CountApprovalInboxes");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountApprovalInboxes", request.getPage().getPageToken());
        return CountApprovalInboxesResponse.getDefaultInstance();
    }

    /**
     * Returns several approval inboxes at once.
     */
    public ResolveApprovalInboxesResponse resolveApprovalInboxes(ResolveApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ResolveApprovalInboxes");
        return ResolveApprovalInboxesResponse.getDefaultInstance();
    }

    /**
     * Streams every matching approval inbox.
     */
    public ExportApprovalInboxesResponse exportApprovalInboxes(ExportApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ExportApprovalInboxes");

        var outbound0 = SubmitCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitCustomerSegmentRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.CustomerSegmentApprovalService");
        var selected0 = CustomerSegmentApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.submitCustomerSegment(outbound0);

        var outbound1 = mapper.exportApprovalInboxesStage1(request, reply0);
        var reply1 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeCloseChecklistWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportApprovalInboxes", request.getFilter().getQuery());
        return ExportApprovalInboxesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCloseChecklist().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .addLines(ApprovalInboxLine.newBuilder()
                                .setTax(ApprovalInboxLineTax.newBuilder()
                                        .setAmount(reply0.getCustomerSegment().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Summarizes an approval inbox.
     */
    public SummarizeApprovalInboxesResponse summarizeApprovalInboxes(SummarizeApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "SummarizeApprovalInboxes");

        var outbound0 = ResolvePageCompositionsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.webbff.v1.PageCompositionQueryService");
        var selected0 = PageCompositionQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.resolvePageCompositions(outbound0);

        var outbound1 = SummarizeVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeVenuesRequest.Details.newBuilder()
                        .putAllFilters(request.getDetails().getFiltersMap())
                        .addAllRecipientEmails(request.getDetails().getRecipientEmailsList())
                        .build())
                .build();
        final class Stage1Adapter {
            SummarizeVenuesResponse fetch(SummarizeVenuesRequest input) {
                return venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeApprovalInboxes", request.getApprovalInboxId());
        return SummarizeApprovalInboxesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public SuggestApprovalInboxesResponse suggestApprovalInboxes(SuggestApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "SuggestApprovalInboxes");

        var outbound0 = JsonCodec.roundTrip(ListAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build(), ListAuditEventsRequest.class);
        var reply0 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestApprovalInboxes", request.getFilter().getOwnerId());
        return SuggestApprovalInboxesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }

    /**
     * Returns an approval inbox.
     */
    public PeekApprovalInboxResponse peekApprovalInbox(PeekApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "PeekApprovalInbox");

        var outbound0 = GenerateTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(GenerateTaxWithholdingReportRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var completion0 = new CompletableFuture<GenerateTaxWithholdingReportResponse>();
        events.publishEvent(new PeekApprovalInboxStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return PeekApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
