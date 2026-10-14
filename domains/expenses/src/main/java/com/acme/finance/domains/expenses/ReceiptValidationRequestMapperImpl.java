package com.acme.finance.domains.expenses;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventMeasurement;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.FunnelRef;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.boxoffice.v1.BoxOfficeSale;
import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.CashDrawerRef;
import com.acme.boxoffice.v1.ExplainBoxOfficeSaleCalculationRequest;
import com.acme.boxoffice.v1.ExplainBoxOfficeSaleCalculationResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.CountComplianceCasesRequest;
import com.acme.compliance.v1.CountComplianceCasesResponse;
import com.acme.compliance.v1.CountControlsRequest;
import com.acme.compliance.v1.CountControlsResponse;
import com.acme.concessions.v1.ConcessionStandQueryServiceGrpc;
import com.acme.concessions.v1.FindConcessionStandsRequest;
import com.acme.concessions.v1.FindConcessionStandsResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.EstimateMileageClaimRequest;
import com.acme.expenses.v1.EstimateMileageClaimResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExplainReceiptDecisionRequest;
import com.acme.expenses.v1.ExplainReceiptDecisionResponse;
import com.acme.expenses.v1.GetReceiptRatesRequest;
import com.acme.expenses.v1.GetReceiptRatesResponse;
import com.acme.expenses.v1.GetReceiptRulesRequest;
import com.acme.expenses.v1.GetReceiptRulesResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimBankSettlement;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ScoreReceiptRequest;
import com.acme.expenses.v1.ScoreReceiptResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ReceiptValidationRequestMapperImpl implements ReceiptValidationRequestMapper {
    @Override
    public CompareLedgerAccountVersionsRequest lintReceiptsStage2(LintReceiptsRequest request) {
        return CompareLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLedgerAccountVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setHolder(PostingBatchRef.newBuilder()
                                .setNumber(request.getReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
    }
}
