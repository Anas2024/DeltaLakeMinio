package com.izycap.deltalake.entities;
import com.izycap.deltalake.annotations.DecimalPrecision;
import com.izycap.deltalake.utils.MyBigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ShopUserTransaction implements Serializable {
    private Long shop_id;
    private String unique_id;
    private String card_token;
    private String customer_id;
    private Long user_id;
    private java.sql.Timestamp purchase_date;
    private java.sql.Date commercial_date;
    private String currency;
    @DecimalPrecision(precision = 10, scale = 2)
    private BigDecimal amount;
    private Integer transaction;
    private Integer year_rank;
    private Boolean member;
    private String transaction_type;
    private Integer new_customer;
    private Integer old_customer;
    private String acceptance;

    public static StructType getSchema() {
        StructType deltaLakeSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("shop_id", DataTypes.LongType, false),
                DataTypes.createStructField("unique_id", DataTypes.StringType, false),
                DataTypes.createStructField("card_token", DataTypes.StringType, true),
                DataTypes.createStructField("customer_id", DataTypes.StringType, false),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("purchase_date", DataTypes.TimestampType, false),
                DataTypes.createStructField("commercial_date", DataTypes.DateType, false),
                DataTypes.createStructField("currency", DataTypes.StringType, false),
                DataTypes.createStructField("amount", DataTypes.createDecimalType(10, 2), false),
                DataTypes.createStructField("transaction", DataTypes.IntegerType, false),
                DataTypes.createStructField("year_rank", DataTypes.IntegerType, true),
                DataTypes.createStructField("member", DataTypes.BooleanType, false),
                DataTypes.createStructField("transaction_type", DataTypes.StringType, false),
                DataTypes.createStructField("new_customer", DataTypes.IntegerType, true),
                DataTypes.createStructField("old_customer", DataTypes.IntegerType, true),
                DataTypes.createStructField("acceptance", DataTypes.StringType, true)
        });
        return deltaLakeSchema;
    }
}