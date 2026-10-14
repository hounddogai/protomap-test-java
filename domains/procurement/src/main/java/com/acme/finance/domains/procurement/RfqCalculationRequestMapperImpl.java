package com.acme.finance.domains.procurement;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class RfqCalculationRequestMapperImpl implements RfqCalculationRequestMapper {
    @Override
    public BrowseRiskScenariosRequest explainRfqCalculationStage1(ExplainRfqCalculationRequest request, EstimateMandateResponse reply0) {
        return BrowseRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseRiskScenariosRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRfq().getAudit().getCreatedAt())
                        .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
