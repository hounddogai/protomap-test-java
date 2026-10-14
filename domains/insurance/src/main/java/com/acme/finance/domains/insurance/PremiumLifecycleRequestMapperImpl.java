package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareConsentRecordOptionsRequest;
import com.acme.customers.v1.CompareConsentRecordOptionsResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainHouseholdCalculationRequest;
import com.acme.customers.v1.ExplainHouseholdCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceBlockAction;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.fleet.v1.ResolveVehiclesRequest;
import com.acme.fleet.v1.ResolveVehiclesResponse;
import com.acme.fleet.v1.VehicleQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePremiumRequest;
import com.acme.insurance.v1.ActivatePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.insurance.v1.ExpirePremiumRequest;
import com.acme.insurance.v1.ExpirePremiumResponse;
import com.acme.insurance.v1.GetPremiumLifecycleRequest;
import com.acme.insurance.v1.GetPremiumLifecycleResponse;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReinstatePremiumRequest;
import com.acme.insurance.v1.ReinstatePremiumResponse;
import com.acme.insurance.v1.RenewPremiumRequest;
import com.acme.insurance.v1.RenewPremiumResponse;
import com.acme.insurance.v1.SchedulePremiumExpiryRequest;
import com.acme.insurance.v1.SchedulePremiumExpiryResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.insurance.v1.SuspendPremiumRequest;
import com.acme.insurance.v1.SuspendPremiumResponse;
import com.acme.insurance.v1.TerminatePremiumRequest;
import com.acme.insurance.v1.TerminatePremiumResponse;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.payments.v1.Payment;
import com.acme.payments.v1.PaymentCalculationServiceGrpc;
import com.acme.payments.v1.RefundRef;
import com.acme.payments.v1.SimulatePaymentRequest;
import com.acme.payments.v1.SimulatePaymentResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PremiumLifecycleRequestMapperImpl implements PremiumLifecycleRequestMapper {
    @Override
    public ComparePreferenceOptionsRequest getPremiumLifecycleStage1(GetPremiumLifecycleRequest request) {
        return ComparePreferenceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
    }
}
