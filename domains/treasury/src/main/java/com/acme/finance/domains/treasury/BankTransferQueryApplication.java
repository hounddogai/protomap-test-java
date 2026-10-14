package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.fuel.v1.EstimateFuelCardRequest;
import com.acme.fuel.v1.EstimateFuelCardResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BrowseBankTransfersRequest;
import com.acme.treasury.v1.BrowseBankTransfersResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.FindBankTransfersRequest;
import com.acme.treasury.v1.FindBankTransfersResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.LookupBankTransferRequest;
import com.acme.treasury.v1.LookupBankTransferResponse;
import com.acme.treasury.v1.PeekBankTransferRequest;
import com.acme.treasury.v1.PeekBankTransferResponse;
import com.acme.treasury.v1.ResolveBankTransfersRequest;
import com.acme.treasury.v1.ResolveBankTransfersResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.SuggestBankTransfersRequest;
import com.acme.treasury.v1.SuggestBankTransfersResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about bank transfers, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BankTransferQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferQueryApplication.class);

    private final FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public BankTransferQueryApplication(
            FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            TreasuryRepository repository,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.fuelFuelCardCalculationServiceBlocking = fuelFuelCardCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    /**
     * Returns a bank transfer.
     */
    public LookupBankTransferResponse lookupBankTransfer(LookupBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "LookupBankTransfer");

        var outbound0 = BatchGetUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.retryRead(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetUsers(outbound0));

        var builder1 = EstimateFuelCardRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFuelCard(FuelCard.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = fuelFuelCardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFuelCard(outbound1);
        return LookupBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getFuelCard().getAudit().getUpdatedAt())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getFuelCard().getHolder().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public BrowseBankTransfersResponse browseBankTransfers(BrowseBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "BrowseBankTransfers");

        var outbound0 = SimulateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.FxHedgeCalculationService/SimulateFxHedge", outbound0);
        var reply0 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateFxHedge(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseBankTransfers", request.getPage().getPageToken());
        return BrowseBankTransfersResponse.newBuilder()
                .addBankTransfers(BankTransfer.newBuilder()
                        .setNotes(reply0.getFxHedge().getNotes())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public FindBankTransfersResponse findBankTransfers(FindBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "FindBankTransfers");

        var outbound0 = ExplainBankTransferCalculationRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBankTransferCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainBankTransferCalculationRequest, ExplainBankTransferCalculationResponse> port0 = RpcPort.adapt(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainBankTransferCalculation);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindBankTransfers", request.getPage().getPageToken());
        return FindBankTransfersResponse.newBuilder()
                .addBankTransfers(BankTransfer.newBuilder()
                        .setBankTransferId(reply0.getBankTransfer().getBankTransferId())
                        .putAllMetadata(reply0.getBankTransfer().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Counts bank transfers by status.
     */
    public CountBankTransfersResponse countBankTransfers(CountBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "CountBankTransfers");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBankTransfers", request.getPage().getPageToken());
        return CountBankTransfersResponse.getDefaultInstance();
    }

    /**
     * Returns several bank transfers at once.
     */
    public ResolveBankTransfersResponse resolveBankTransfers(ResolveBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "ResolveBankTransfers");

        var outbound0 = BeanCopies.copy(CheckCashPositionEligibilityRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getContext().getActor().getUserId())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckCashPositionEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build(), CheckCashPositionEligibilityRequest.class);
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCashPositionEligibility(outbound0);

        var outbound1 = ExplainBankTransferCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainBankTransferCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getCashPosition().getContact().getPhone())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ExplainBankTransferCalculationResponse fetch(ExplainBankTransferCalculationRequest input) {
                return treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBankTransferCalculation(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        return ResolveBankTransfersResponse.newBuilder()
                .addBankTransfers(BankTransfer.newBuilder()
                        .setNumber(reply0.getCashPosition().getHolder().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public SuggestBankTransfersResponse suggestBankTransfers(SuggestBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "SuggestBankTransfers");

        var outbound0 = ValidateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCashPosition(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestBankTransfers", request.getPage().getPageToken());
        return SuggestBankTransfersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a bank transfer.
     */
    public PeekBankTransferResponse peekBankTransfer(PeekBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "PeekBankTransfer");
        return PeekBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .build();
    }
}
