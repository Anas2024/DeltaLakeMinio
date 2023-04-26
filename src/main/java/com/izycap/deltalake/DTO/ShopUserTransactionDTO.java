package com.izycap.deltalake.DTO;

import com.izycap.deltalake.annotations.DecimalPrecision;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ShopUserTransactionDTO implements Serializable {
    private String unique_id;
    private String card_token;
    private String customer_id;
    private Long user_id;
    private java.sql.Timestamp purchase_date;
    private java.sql.Date commercial_date;
    private String currency;
    @DecimalPrecision(precision = 10, scale = 2)
    private java.math.BigDecimal amount;
    private Integer transaction;
    private Integer year_rank;
    private Boolean member;
    private String transaction_type;
    private Integer new_customer;
    private Integer old_customer;
    private String acceptance;
}