package com.izycap.deltalake;

import com.izycap.deltalake.config.SparkSessionPool;
import com.izycap.deltalake.entities.ShopUserTransaction;
import com.izycap.deltalake.service.ShopUserTransactionService;
import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DeltaLakeSparkMinioV4Application {

    public static void main(String[] args) {
        SpringApplication.run(DeltaLakeSparkMinioV4Application.class, args);
    }
    @Bean
    ApplicationRunner applicationRunner(SparkSessionPool sparkSessionPool, @Value("${delta.tables.ShopUserTransactionPath}") String shopUserTransactionPath)
    {
        return args -> {
            SparkSession sparkSession = null;
            try {
                sparkSession = sparkSessionPool.borrowSparkSession();

                //create delta tables with default structure
                DeltaTableBuilder deltaTable = DeltaTable.createIfNotExists(sparkSession).location(shopUserTransactionPath).addColumns(ShopUserTransaction.getSchema());
                deltaTable.execute();
                /*Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath);
                df.printSchema();*/
            } catch (Exception e) {
                throw new RuntimeException("Failed to create Delta table 'shop_user_transaction'", e);
            } finally {
                if (sparkSession != null) {
                    sparkSessionPool.returnSparkSession(sparkSession);
                }
            }

        };
    }
}
