package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderWorkflowServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the procurement contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ProcurementClients {
    @Bean
    public GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking(ChannelRegistry channels) {
        return GoodsReceiptApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptApprovalService"));
    }

    @Bean
    public GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceFutureStub procurementGoodsReceiptApprovalServiceFuture(ChannelRegistry channels) {
        return GoodsReceiptApprovalServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.GoodsReceiptApprovalService"));
    }

    @Bean
    public GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking(ChannelRegistry channels) {
        return GoodsReceiptCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptCalculationService"));
    }

    @Bean
    public GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceBlockingStub procurementGoodsReceiptLifecycleServiceBlocking(ChannelRegistry channels) {
        return GoodsReceiptLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptLifecycleService"));
    }

    @Bean
    public GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking(ChannelRegistry channels) {
        return GoodsReceiptValidationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptValidationService"));
    }

    @Bean
    public GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceFutureStub procurementGoodsReceiptValidationServiceFuture(ChannelRegistry channels) {
        return GoodsReceiptValidationServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.GoodsReceiptValidationService"));
    }

    @Bean
    public GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceStub procurementGoodsReceiptWorkflowServiceAsync(ChannelRegistry channels) {
        return GoodsReceiptWorkflowServiceGrpc.newStub(channels.lookup("acme.procurement.v1.GoodsReceiptWorkflowService"));
    }

    @Bean
    public GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceBlockingStub procurementGoodsReceiptWorkflowServiceBlocking(ChannelRegistry channels) {
        return GoodsReceiptWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptWorkflowService"));
    }

    @Bean
    public PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderApprovalService"));
    }

    @Bean
    public PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderCalculationService"));
    }

    @Bean
    public PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderLifecycleService"));
    }

    @Bean
    public PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderQueryServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderQueryService"));
    }

    @Bean
    public PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceStub procurementPurchaseOrderValidationServiceAsync(ChannelRegistry channels) {
        return PurchaseOrderValidationServiceGrpc.newStub(channels.lookup("acme.procurement.v1.PurchaseOrderValidationService"));
    }

    @Bean
    public PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderValidationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderValidationService"));
    }

    @Bean
    public PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceFutureStub procurementPurchaseOrderValidationServiceFuture(ChannelRegistry channels) {
        return PurchaseOrderValidationServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.PurchaseOrderValidationService"));
    }

    @Bean
    public PurchaseOrderWorkflowServiceGrpc.PurchaseOrderWorkflowServiceBlockingStub procurementPurchaseOrderWorkflowServiceBlocking(ChannelRegistry channels) {
        return PurchaseOrderWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseOrderWorkflowService"));
    }

    @Bean
    public PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceBlockingStub procurementPurchaseRequisitionApprovalServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionApprovalService"));
    }

    @Bean
    public PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionCalculationService"));
    }

    @Bean
    public PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceFutureStub procurementPurchaseRequisitionCalculationServiceFuture(ChannelRegistry channels) {
        return PurchaseRequisitionCalculationServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionCalculationService"));
    }

    @Bean
    public PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionQueryServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionQueryService"));
    }

    @Bean
    public PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceFutureStub procurementPurchaseRequisitionQueryServiceFuture(ChannelRegistry channels) {
        return PurchaseRequisitionQueryServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionQueryService"));
    }

    @Bean
    public PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync(ChannelRegistry channels) {
        return PurchaseRequisitionServiceGrpc.newStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionService"));
    }

    @Bean
    public PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionService"));
    }

    @Bean
    public PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceFutureStub procurementPurchaseRequisitionServiceFuture(ChannelRegistry channels) {
        return PurchaseRequisitionServiceGrpc.newFutureStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionService"));
    }

    @Bean
    public PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceStub procurementPurchaseRequisitionValidationServiceAsync(ChannelRegistry channels) {
        return PurchaseRequisitionValidationServiceGrpc.newStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionValidationService"));
    }

    @Bean
    public PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionValidationService"));
    }

    @Bean
    public PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking(ChannelRegistry channels) {
        return PurchaseRequisitionWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.PurchaseRequisitionWorkflowService"));
    }

    @Bean
    public RfqApprovalServiceGrpc.RfqApprovalServiceBlockingStub procurementRfqApprovalServiceBlocking(ChannelRegistry channels) {
        return RfqApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.RfqApprovalService"));
    }

    @Bean
    public RfqCalculationServiceGrpc.RfqCalculationServiceStub procurementRfqCalculationServiceAsync(ChannelRegistry channels) {
        return RfqCalculationServiceGrpc.newStub(channels.lookup("acme.procurement.v1.RfqCalculationService"));
    }

    @Bean
    public RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking(ChannelRegistry channels) {
        return RfqCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.RfqCalculationService"));
    }

    @Bean
    public RfqLifecycleServiceGrpc.RfqLifecycleServiceBlockingStub procurementRfqLifecycleServiceBlocking(ChannelRegistry channels) {
        return RfqLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.RfqLifecycleService"));
    }

    @Bean
    public RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking(ChannelRegistry channels) {
        return RfqReportingServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.RfqReportingService"));
    }

    @Bean
    public RfqValidationServiceGrpc.RfqValidationServiceStub procurementRfqValidationServiceAsync(ChannelRegistry channels) {
        return RfqValidationServiceGrpc.newStub(channels.lookup("acme.procurement.v1.RfqValidationService"));
    }

    @Bean
    public RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking(ChannelRegistry channels) {
        return RfqValidationServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.RfqValidationService"));
    }
}
