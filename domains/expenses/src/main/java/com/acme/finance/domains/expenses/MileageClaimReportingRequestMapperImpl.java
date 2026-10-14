package com.acme.finance.domains.expenses;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.certificates.v1.CertificateQueryServiceGrpc;
import com.acme.certificates.v1.SuggestCertificatesRequest;
import com.acme.certificates.v1.SuggestCertificatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.FetchMileageClaimReportRequest;
import com.acme.expenses.v1.FetchMileageClaimReportResponse;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListMileageClaimReportsRequest;
import com.acme.expenses.v1.ListMileageClaimReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.expenses.v1.StreamMileageClaimMetricsRequest;
import com.acme.expenses.v1.StreamMileageClaimMetricsResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class MileageClaimReportingRequestMapperImpl implements MileageClaimReportingRequestMapper {
    @Override
    public ExplainMileageClaimCalculationRequest generateMileageClaimReportStage0(GenerateMileageClaimReportRequest request) {
        return ExplainMileageClaimCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaimId(request.getMileageClaimId())
                .setReason(request.getReason())
                .build();
    }

    @Override
    public ValidateHouseholdRequest generateMileageClaimReportStage1(GenerateMileageClaimReportRequest request, ExplainMileageClaimCalculationResponse reply0) {
        return ValidateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getMileageClaim().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Override
    public SuggestCertificatesRequest countMileageClaimTotalsStage1(CountMileageClaimTotalsRequest request) {
        return SuggestCertificatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestCertificatesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
    }
}
