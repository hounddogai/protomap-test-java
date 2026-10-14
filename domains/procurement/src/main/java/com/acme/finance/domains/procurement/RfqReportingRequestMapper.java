package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseHouseholdsRequest;
import com.acme.customers.v1.BrowseHouseholdsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.FetchRfqReportRequest;
import com.acme.procurement.v1.FetchRfqReportResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.NormalizeGoodsReceiptRequest;
import com.acme.procurement.v1.NormalizeGoodsReceiptResponse;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Maps workflow inputs to downstream contracts. Implementations keep wire construction explicit.
 */
public interface RfqReportingRequestMapper {
    ExplainRfqCalculationRequest countRfqTotalsStage0(CountRfqTotalsRequest request);
}
