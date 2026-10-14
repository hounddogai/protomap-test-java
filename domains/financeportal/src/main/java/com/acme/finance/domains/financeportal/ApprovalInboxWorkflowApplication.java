package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationWorkflowServiceGrpc;
import com.acme.accounting.v1.StartAllocationWorkflowRequest;
import com.acme.accounting.v1.StartAllocationWorkflowResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.PauseAssetTransferWorkflowRequest;
import com.acme.assets.v1.PauseAssetTransferWorkflowResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerApprovalServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentSyncServiceGrpc;
import com.acme.customers.v1.PullCustomerSegmentsRequest;
import com.acme.customers.v1.PullCustomerSegmentsResponse;
import com.acme.customers.v1.RecallCustomerRequest;
import com.acme.customers.v1.RecallCustomerResponse;
import com.acme.driverapp.v1.DriverHomeQueryServiceGrpc;
import com.acme.driverapp.v1.ResolveDriverHomesRequest;
import com.acme.driverapp.v1.ResolveDriverHomesResponse;
import com.acme.edgegateway.v1.BatchGetRouteBindingsRequest;
import com.acme.edgegateway.v1.BatchGetRouteBindingsResponse;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.employeeportal.v1.FindPortalHomesRequest;
import com.acme.employeeportal.v1.FindPortalHomesResponse;
import com.acme.employeeportal.v1.PortalHomeQueryServiceGrpc;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowResponse;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationAttachment;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.patientapp.v1.BrowsePatientHomesRequest;
import com.acme.patientapp.v1.BrowsePatientHomesResponse;
import com.acme.patientapp.v1.PatientHomeQueryServiceGrpc;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes an approval inbox from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ApprovalInboxWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ApprovalInboxWorkflowApplication.class);

    private final AllocationWorkflowServiceGrpc.AllocationWorkflowServiceBlockingStub accountingAllocationWorkflowServiceBlocking;

    private final AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceBlockingStub assetsAssetTransferWorkflowServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final CustomerApprovalServiceGrpc.CustomerApprovalServiceBlockingStub customersCustomerApprovalServiceBlocking;

    private final CustomerSegmentSyncServiceGrpc.CustomerSegmentSyncServiceBlockingStub customersCustomerSegmentSyncServiceBlocking;

    private final DriverHomeQueryServiceGrpc.DriverHomeQueryServiceBlockingStub driverappDriverHomeQueryServiceBlocking;

    private final RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking;

    private final PortalHomeQueryServiceGrpc.PortalHomeQueryServiceBlockingStub employeeportalPortalHomeQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final PatientHomeQueryServiceGrpc.PatientHomeQueryServiceBlockingStub patientappPatientHomeQueryServiceBlocking;

    private final FinanceportalRepository repository;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public ApprovalInboxWorkflowApplication(
            AllocationWorkflowServiceGrpc.AllocationWorkflowServiceBlockingStub accountingAllocationWorkflowServiceBlocking,
            AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceBlockingStub assetsAssetTransferWorkflowServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            CustomerApprovalServiceGrpc.CustomerApprovalServiceBlockingStub customersCustomerApprovalServiceBlocking,
            CustomerSegmentSyncServiceGrpc.CustomerSegmentSyncServiceBlockingStub customersCustomerSegmentSyncServiceBlocking,
            DriverHomeQueryServiceGrpc.DriverHomeQueryServiceBlockingStub driverappDriverHomeQueryServiceBlocking,
            RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking,
            PortalHomeQueryServiceGrpc.PortalHomeQueryServiceBlockingStub employeeportalPortalHomeQueryServiceBlocking,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            PatientHomeQueryServiceGrpc.PatientHomeQueryServiceBlockingStub patientappPatientHomeQueryServiceBlocking,
            FinanceportalRepository repository,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.accountingAllocationWorkflowServiceBlocking = accountingAllocationWorkflowServiceBlocking;
        this.assetsAssetTransferWorkflowServiceBlocking = assetsAssetTransferWorkflowServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.customersCustomerApprovalServiceBlocking = customersCustomerApprovalServiceBlocking;
        this.customersCustomerSegmentSyncServiceBlocking = customersCustomerSegmentSyncServiceBlocking;
        this.driverappDriverHomeQueryServiceBlocking = driverappDriverHomeQueryServiceBlocking;
        this.edgegatewayRouteBindingServiceBlocking = edgegatewayRouteBindingServiceBlocking;
        this.employeeportalPortalHomeQueryServiceBlocking = employeeportalPortalHomeQueryServiceBlocking;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.patientappPatientHomeQueryServiceBlocking = patientappPatientHomeQueryServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    public record PauseApprovalInboxWorkflowStage0Requested(ListAuditTrailConflictsRequest request, CompletableFuture<ListAuditTrailConflictsResponse> completion, io.grpc.Context context) {}

    /**
     * Starts an approval inbox.
     */
    public StartApprovalInboxWorkflowResponse startApprovalInboxWorkflow(StartApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var outbound0 = EscalateRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getApprovalInbox().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateRiskScenario(outbound0), rpcExecutor);

        var outbound1 = PauseAssetTransferWorkflowRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getApprovalInbox().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(AssetTransferCardSettlement.newBuilder()
                                .setLast4(request.getApprovalInbox().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PauseAssetTransferWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsAssetTransferWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseAssetTransferWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = EscalateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .setBank(LoanBankFunding.newBuilder()
                                .setIban(switch (reply1.getAssetTransfer().getSettlementCase()) {
                                    case BANK -> reply1.getAssetTransfer().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getRiskScenario().getRequester().getName())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateLoan(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return StartApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getAssetTransfer().getBillingAddress().getCountryCode())
                                .setPostalCode(reply1.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .setDueAt(request.getApprovalInbox().getDueAt())
                        .build())
                .setOutcome(StartApprovalInboxWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances an approval inbox.
     */
    public AdvanceApprovalInboxWorkflowResponse advanceApprovalInboxWorkflow(AdvanceApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        List<PullCustomerSegmentsResponse> results0 = request.getApprovalInbox().getLinesList().stream()
                .map(element -> PullCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getApprovalInbox().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(PullCustomerSegmentsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerSegmentSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullCustomerSegments(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PullCustomerSegmentsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BrowsePatientHomesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePatientHomesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getApprovalInbox().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setRegion(request.getApprovalInbox().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = patientappPatientHomeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePatientHomes(outbound1);

        var outbound2 = RecallSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .addLines(SupplierRatingLine.newBuilder()
                                .setTax(SupplierRatingLineTax.newBuilder()
                                        .setAmount(reply0.getCustomerSegment().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            RecallSupplierRatingResponse fetch(RecallSupplierRatingRequest input) {
                return suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallSupplierRating(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = (ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareFiscalPeriodExport(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return AdvanceApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getSupplierRating().getBillingAddress().getRegion())
                                .build())
                        .setDueAt(request.getApprovalInbox().getDueAt())
                        .build())
                .setOutcome(AdvanceApprovalInboxWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels an approval inbox.
     */
    public CancelApprovalInboxWorkflowResponse cancelApprovalInboxWorkflow(CancelApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        List<CloneVenueResponse> results0 = request.getApprovalInbox().getLinesList().stream()
                .map(element -> CloneVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getApprovalInbox().getAudit().getCreatedAt())
                                .build())
                        .putComponents("primary", VenueComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneVenue(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CloneVenueResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getVenue().getAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createUser(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(ResolveDriverHomesRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveDriverHomesRequest.class);
        var reply2 = driverappDriverHomeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveDriverHomes(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return CancelApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setDueAt(request.getApprovalInbox().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys an approval inbox.
     */
    public RetryApprovalInboxWorkflowResponse retryApprovalInboxWorkflow(RetryApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var futures0 = request.getApprovalInbox().getAttachmentsList().stream()
                .map(element -> StartLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addAttachments(LoanApplicationAttachment.newBuilder()
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startLoanApplicationWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? StartLoanApplicationWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoanApplication().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getLoanApplication().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getApprovalInbox().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = RpcSupport.unpack(Any.pack(FindPortalHomesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPortalHomesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getRetentionPolicy().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getLoanApplication().getMetadataMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build()), FindPortalHomesRequest.class);
        var reply2 = employeeportalPortalHomeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPortalHomes(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return RetryApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(request.getApprovalInbox().getBillingAddress())
                        .build())
                .setOutcome(RetryApprovalInboxWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Pauses an approval inbox.
     */
    public PauseApprovalInboxWorkflowResponse pauseApprovalInboxWorkflow(PauseApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var outbound0 = ListAuditTrailConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getApprovalInbox().getAudit().getUpdatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getApprovalInbox().getApprovalInboxId())
                        .setRegion(request.getApprovalInbox().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListAuditTrailConflictsResponse>();
        events.publishEvent(new PauseApprovalInboxWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = (CloneUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneUser(outbound1);

        var outbound2 = StartAllocationWorkflowRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getApprovalInbox().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getUser().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(StartAllocationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> accountingAllocationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startAllocationWorkflow(value));
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseApprovalInboxWorkflow", request.getApprovalInbox().getNumber());
        return PauseApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getAllocation().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getUser().getPrimaryAddress().getLinesList())
                                .build())
                        .setCounterparty(CloseChecklistRef.newBuilder()
                                .setNumber(reply1.getUser().getPassport().getNumber())
                                .build())
                        .setDueAt(request.getApprovalInbox().getDueAt())
                        .build())
                .setOutcome(PauseApprovalInboxWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes an approval inbox.
     */
    public ResumeApprovalInboxWorkflowResponse resumeApprovalInboxWorkflow(ResumeApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var futures0 = request.getApprovalInbox().getLinesList().stream()
                .map(element -> RecallCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersCustomerApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallCustomer(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RecallCustomerResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SubmitCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCustomer().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setDetails(SubmitCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getApprovalInbox().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<SubmitCoverageRequest, SubmitCoverageResponse> operation1 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitCoverage;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getApprovalInbox().getAudit().getCreatedAt())
                        .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var stream2 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound2);
        var reply2 = ExportUsersResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return ResumeApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setNotes(request.getApprovalInbox().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an approval inbox.
     */
    public InspectApprovalInboxWorkflowResponse inspectApprovalInboxWorkflow(InspectApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "InspectApprovalInboxWorkflow");
        return InspectApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public ListApprovalInboxWorkflowsResponse listApprovalInboxWorkflows(ListApprovalInboxWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListApprovalInboxWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ListApprovalInboxWorkflows");

        var outbound0 = BatchGetRouteBindingsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            BatchGetRouteBindingsResponse fetch(BatchGetRouteBindingsRequest input) {
                return edgegatewayRouteBindingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetRouteBindings(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListApprovalInboxWorkflows", request.getPage().getPageToken());
        return ListApprovalInboxWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals an approval inbox.
     */
    public SignalApprovalInboxWorkflowResponse signalApprovalInboxWorkflow(SignalApprovalInboxWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalApprovalInboxWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var results0 = new ArrayList<PullCustomerSegmentsResponse>();
        for (var element : request.getApprovalInbox().getLinesList()) {
            PullCustomerSegmentsRequest itemRequest = PullCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .setDetails(PullCustomerSegmentsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
            results0.add(customersCustomerSegmentSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullCustomerSegments(itemRequest));
        }
        var reply0 = results0.isEmpty() ? PullCustomerSegmentsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(CreateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getApprovalInbox().getAudit().getEtag())
                                .build())
                        .build())
                .build(), CreateLedgerAccountRequest.class);
        var reply1 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createLedgerAccount(outbound1);

        var outbound2 = (SendJournalEntryTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendJournalEntryTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .setName(reply1.getLedgerAccount().getContact().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(reply1.getLedgerAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendJournalEntryTestNotice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalApprovalInboxWorkflow", request.getApprovalInbox().getApprovalInboxId());
        return SignalApprovalInboxWorkflowResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .setTotal(request.getApprovalInbox().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }
}
