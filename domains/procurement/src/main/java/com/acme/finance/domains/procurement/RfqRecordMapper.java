package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.Rfq;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RfqRecordMapper {
    RfqRecord toRecord(Rfq value);

    Rfq toProto(RfqRecord value);
}
