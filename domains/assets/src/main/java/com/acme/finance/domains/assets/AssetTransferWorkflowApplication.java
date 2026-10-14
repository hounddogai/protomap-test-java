package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceAssetTransferWorkflowRequest;
import com.acme.assets.v1.AdvanceAssetTransferWorkflowResponse;
import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CancelAssetTransferWorkflowRequest;
import com.acme.assets.v1.CancelAssetTransferWorkflowResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.GenerateAssetTransferReportRequest;
import com.acme.assets.v1.GenerateAssetTransferReportResponse;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.InspectAssetTransferWorkflowRequest;
import com.acme.assets.v1.InspectAssetTransferWorkflowResponse;
import com.acme.assets.v1.ListAssetTransferWorkflowsRequest;
import com.acme.assets.v1.ListAssetTransferWorkflowsResponse;
import com.acme.assets.v1.PauseAssetTransferWorkflowRequest;
import com.acme.assets.v1.PauseAssetTransferWorkflowResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.RecallFixedAssetRequest;
import com.acme.assets.v1.RecallFixedAssetResponse;
import com.acme.assets.v1.ResumeAssetTransferWorkflowRequest;
import com.acme.assets.v1.ResumeAssetTransferWorkflowResponse;
import com.acme.assets.v1.SignalAssetTransferWorkflowRequest;
import com.acme.assets.v1.SignalAssetTransferWorkflowResponse;
import com.acme.assets.v1.StartAssetTransferWorkflowRequest;
import com.acme.assets.v1.StartAssetTransferWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.employees.v1.Employee;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.employees.v1.UpdateEmployeeRequest;
import com.acme.employees.v1.UpdateEmployeeResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.lending.v1.SignalLoanWorkflowRequest;
import com.acme.lending.v1.SignalLoanWorkflowResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.seating.v1.CloneSeatMapRequest;
import com.acme.seating.v1.CloneSeatMapResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapServiceGrpc;
import com.acme.sponsorships.v1.CreateSponsorRequest;
import com.acme.sponsorships.v1.CreateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorServiceGrpc;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Runs the long-lived workflow that takes an asset transfer from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AssetTransferWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferWorkflowApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceFutureStub insuranceCoverageApprovalServiceFuture;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    private final SeatMapServiceGrpc.SeatMapServiceBlockingStub seatingSeatMapServiceBlocking;

    private final SponsorServiceGrpc.SponsorServiceBlockingStub sponsorshipsSponsorServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public AssetTransferWorkflowApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            ChannelRegistry channels,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceFutureStub insuranceCoverageApprovalServiceFuture,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SeatMapServiceGrpc.SeatMapServiceBlockingStub seatingSeatMapServiceBlocking,
            SponsorServiceGrpc.SponsorServiceBlockingStub sponsorshipsSponsorServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsAssetTransferReportingServiceBlocking = assetsAssetTransferReportingServiceBlocking;
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.channels = channels;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.employeesEmployeeServiceBlocking = employeesEmployeeServiceBlocking;
        this.insuranceCoverageApprovalServiceFuture = insuranceCoverageApprovalServiceFuture;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.seatingSeatMapServiceBlocking = seatingSeatMapServiceBlocking;
        this.sponsorshipsSponsorServiceBlocking = sponsorshipsSponsorServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Starts an asset transfer.
     */
    public StartAssetTransferWorkflowResponse startAssetTransferWorkflow(StartAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RecallFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallFixedAssetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getAssetTransfer().getBillingAddress().getCountryCode())
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallFixedAsset(outbound0), rpcExecutor);

        var outbound1 = QuoteAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCard(AssetTransferCardSettlement.newBuilder()
                                .setLast4(request.getAssetTransfer().getCard().getLast4())
                                .setToken(request.getAssetTransfer().getCard().getToken())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteAssetTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getAssetTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteAssetTransfer(outbound1), rpcExecutor);

        var outbound2 = BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDocuments(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartAssetTransferWorkflow", request.getAssetTransfer().getAssetTransferId());
        return StartAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getAssetTransfer().getBillingAddress().getRecipient())
                                .build())
                        .setIssuedAt(request.getAssetTransfer().getIssuedAt())
                        .build())
                .setOutcome(StartAssetTransferWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances an asset transfer.
     */
    public AdvanceAssetTransferWorkflowResponse advanceAssetTransferWorkflow(AdvanceAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var builder0 = ExplainVenueDecisionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setVenue(Venue.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(request.getAssetTransfer().getAudit().getEtag())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVenueDecision(outbound0);

        var outbound1 = CreateSponsorRequest.newBuilder()
                .setContext(request.getContext())
                .setSponsor(Sponsor.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = sponsorshipsSponsorServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSponsor(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceAssetTransferWorkflow", request.getAssetTransfer().getAssetTransferId());
        return AdvanceAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getVenue().getAddress().getLinesList())
                                .setLocality(reply0.getVenue().getAddress().getLocality())
                                .build())
                        .putAllMetadata(request.getAssetTransfer().getMetadataMap())
                        .setNumber(reply1.getSponsor().getPassport().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Cancels an asset transfer.
     */
    public CancelAssetTransferWorkflowResponse cancelAssetTransferWorkflow(CancelAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = JsonCodec.roundTrip(CloneSeatMapRequest.newBuilder()
                .setContext(request.getContext())
                .setSeatMap(SeatMap.newBuilder()
                        .addChildren(SeatMap.newBuilder()
                                .addChildren(SeatMap.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(request.getAssetTransfer().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), CloneSeatMapRequest.class);
        var reply0 = seatingSeatMapServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneSeatMap(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(MapUnderwritingImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getSeatMap().getManager().getDisplayName())
                                .setEmail(reply0.getSeatMap().getManager().getEmail())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()), MapUnderwritingImportColumnsRequest.class);
        var reply1 = insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).mapUnderwritingImportColumns(outbound1);

        var outbound2 = EscalateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getSeatMap().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var pending2 = insuranceCoverageApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).escalateCoverage(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = (NormalizeJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getSeatMap().getManager().getEmail())
                                .setPreferredChannel(reply1.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeJournalEntry(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAssetTransferWorkflow", request.getAssetTransfer().getAssetTransferId());
        return CancelAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getJournalEntry().getAudit().getCreatedAt())
                                .build())
                        .setStatus(request.getAssetTransfer().getStatus())
                        .build())
                .setOutcome(CancelAssetTransferWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Pauses an asset transfer.
     */
    public PauseAssetTransferWorkflowResponse pauseAssetTransferWorkflow(PauseAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = JsonCodec.roundTrip(SignalLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .build())
                        .setBank(LoanBankFunding.newBuilder()
                                .setIban(request.getAssetTransfer().getBank().getIban())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build(), SignalLoanWorkflowRequest.class);
        var reply0 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalLoanWorkflow(outbound0);

        var outbound1 = GetAssetTransferSyncStateRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransfer().getAssetTransferId())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            GetAssetTransferSyncStateResponse fetch(GetAssetTransferSyncStateRequest input) {
                return assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAssetTransferSyncState(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseAssetTransferWorkflow", request.getAssetTransfer().getNumber());
        return PauseAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setTotal(request.getAssetTransfer().getTotal())
                        .build())
                .build();
    }

    /**
     * Resumes an asset transfer.
     */
    public ResumeAssetTransferWorkflowResponse resumeAssetTransferWorkflow(ResumeAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ListFiscalPeriodVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getDueAt())
                        .setOwnerId(request.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.FiscalPeriodHistoryService");
        var serviceName0 = "acme.ledger.v1" + "." + "FiscalPeriodHistoryService";
        var methodName0 = "Lis" + "tFiscalPeriodVersions";
        var descriptor0 = MethodDescriptor.<ListFiscalPeriodVersionsRequest, ListFiscalPeriodVersionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ListFiscalPeriodVersionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListFiscalPeriodVersionsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CreateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .build())
                        .setCounterparty(DocumentFolderRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDocument(outbound1);

        var outbound2 = (UpdateEmployeeRequest.newBuilder()
                .setContext(request.getContext())
                .setEmployee(Employee.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getDocument().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = employeesEmployeeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateEmployee(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeAssetTransferWorkflow", request.getAssetTransfer().getAssetTransferId());
        return ResumeAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getEmployee().getPrimaryAddress().getPostalCode())
                                .setRegion(reply2.getEmployee().getPrimaryAddress().getRegion())
                                .build())
                        .putAllMetadata(reply1.getDocument().getMetadataMap())
                        .setNotes(reply1.getDocument().getNotes())
                        .setTotal(request.getAssetTransfer().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public InspectAssetTransferWorkflowResponse inspectAssetTransferWorkflow(InspectAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "InspectAssetTransferWorkflow");

        var outbound0 = ApproveFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveFixedAssetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(request.getAssetTransferId())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ApproveFixedAssetResponse fetch(ApproveFixedAssetRequest input) {
                return assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveFixedAsset(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return InspectAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists asset transfers a page at a time.
     */
    public ListAssetTransferWorkflowsResponse listAssetTransferWorkflows(ListAssetTransferWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAssetTransferWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "ListAssetTransferWorkflows");

        var outbound0 = ShareRepaymentReportRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(ShareRepaymentReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRepaymentReport(value)))
                .collect(Collectors.toList()).getFirst();

        var builder1 = BrowseMetricSeriesesRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                .putAllLabels(request.getFilter().getLabelsMap())
                .setRegion(reply0.getRepayment().getLocation().getRegion())
                .build());
        builder1.setPage(PageRequest.newBuilder()
                .setPageSize(request.getPage().getPageSize())
                .setPageToken(request.getPage().getPageToken())
                .build());
        var outbound1 = builder1.build();
        var reply1 = metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMetricSerieses(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAssetTransferWorkflows", request.getFilter().getQuery());
        return ListAssetTransferWorkflowsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply1.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals an asset transfer.
     */
    public SignalAssetTransferWorkflowResponse signalAssetTransferWorkflow(SignalAssetTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalAssetTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RpcSupport.unpack(Any.pack(GenerateAssetTransferReportRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransfer().getAssetTransferId())
                .setContext(request.getContext())
                .build()), GenerateAssetTransferReportRequest.class);
        var reply0 = assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateAssetTransferReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalAssetTransferWorkflow", request.getAssetTransfer().getNumber());
        return SignalAssetTransferWorkflowResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setStatus(request.getAssetTransfer().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
