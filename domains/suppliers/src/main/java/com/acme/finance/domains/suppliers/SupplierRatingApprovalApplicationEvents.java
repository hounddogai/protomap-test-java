package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class SupplierRatingApprovalApplicationEvents {
    private final AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceStub assetsAssetTransferApprovalService;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceStub customersCustomerSegmentValidationService;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceStub suppliersSupplierContractQueryService;

    public SupplierRatingApprovalApplicationEvents(AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceStub assetsAssetTransferApprovalService, CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceStub customersCustomerSegmentValidationService, SupplierContractQueryServiceGrpc.SupplierContractQueryServiceStub suppliersSupplierContractQueryService) {
        this.assetsAssetTransferApprovalService = assetsAssetTransferApprovalService;
        this.customersCustomerSegmentValidationService = customersCustomerSegmentValidationService;
        this.suppliersSupplierContractQueryService = suppliersSupplierContractQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SupplierRatingApprovalApplication.SubmitSupplierRatingStage3Requested event) {
        event.context().run(() -> {
            try {
                suppliersSupplierContractQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resolveSupplierContracts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SupplierRatingApprovalApplication.EscalateSupplierRatingStage0Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerSegmentValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateCustomerSegment(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SupplierRatingApprovalApplication.GetSupplierRatingApprovalStatusStage1Requested event) {
        event.context().run(() -> {
            try {
                assetsAssetTransferApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .submitAssetTransfer(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
