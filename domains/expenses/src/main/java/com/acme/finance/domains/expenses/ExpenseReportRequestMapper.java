package com.acme.finance.domains.expenses;

import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.BatchGetExpenseReportsRequest;
import com.acme.expenses.v1.BatchGetExpenseReportsResponse;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.CloneExpenseReportRequest;
import com.acme.expenses.v1.CloneExpenseReportResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.CreateExpenseReportRequest;
import com.acme.expenses.v1.CreateExpenseReportResponse;
import com.acme.expenses.v1.DeleteExpenseReportRequest;
import com.acme.expenses.v1.DeleteExpenseReportResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.GetExpenseReportRequest;
import com.acme.expenses.v1.GetExpenseReportResponse;
import com.acme.expenses.v1.ListExpenseReportsRequest;
import com.acme.expenses.v1.ListExpenseReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.SearchExpenseReportsRequest;
import com.acme.expenses.v1.SearchExpenseReportsResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.expenses.v1.WatchExpenseReportsRequest;
import com.acme.expenses.v1.WatchExpenseReportsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Maps workflow inputs to downstream contracts. Implementations keep wire construction explicit.
 */
public interface ExpenseReportRequestMapper {
    CompareMileageClaimOptionsRequest createExpenseReportStage2(CreateExpenseReportRequest request, CalculateExpensePolicyResponse reply1);

    BrowseSearchIndexesRequest updateExpenseReportStage1(UpdateExpenseReportRequest request, ScoreUnderwritingResponse reply0);

    EstimateFixedAssetRequest watchExpenseReportsStage1(WatchExpenseReportsRequest request, CalculateExpenseReportResponse reply0);
}
