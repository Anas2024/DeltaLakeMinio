package com.izycap.deltalake.impl;

import com.izycap.deltalake.DTO.ShopUserTransactionDTO;
import com.izycap.deltalake.config.SparkSessionPool;
import com.izycap.deltalake.entities.ShopUserTransaction;
import com.izycap.deltalake.service.ShopUserTransactionService;
import com.izycap.deltalake.utils.DateUtil;
import com.izycap.deltalake.utils.ModelMapperUtils;
import com.izycap.deltalake.utils.MyBigDecimal;
import io.delta.standalone.DeltaLog;
import io.delta.tables.DeltaTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import scala.collection.immutable.Map;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.spark.sql.functions.col;

@Slf4j
@Service
@Qualifier("ShopUserTransactionImpl")
public class ShopUserTransactionImpl implements ShopUserTransactionService {
    private final SparkSessionPool sparkSessionPool;
    @Value("${delta.tables.ShopUserTransactionPath}")
    private String shopUserTransactionPath;


    @Autowired
    public ShopUserTransactionImpl(SparkSessionPool sparkSessionPool) {
        this.sparkSessionPool = sparkSessionPool;
    }

    @Override
    @Transactional
    public void insertData(Integer numberOfData)
    {
        Integer rowNumber = numberOfData != null ? numberOfData : 100;
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            List<ShopUserTransaction> deltaLakeData = new ArrayList<>();
            Random rand = new Random();
            Dataset<Row> deltaDF = sparkSession.read().format("delta").load(shopUserTransactionPath);
            Object maxIdObj = deltaDF.agg(functions.max("shop_id")).head().get(0);
            Long maxId = maxIdObj != null ? ((Long) maxIdObj) : 0;
            for (long i = ++maxId; i < (maxId+rowNumber); i++) {
                Long shop_id = i;
                String unique_id = "UID_" + i;
                String card_token = (rand.nextBoolean()) ? "CARD_" + i : null;
                String customer_id = "CUST_" + i;
                Long user_id = (rand.nextBoolean()) ? Math.abs(rand.nextLong()) : null;
                Timestamp purchase_date = new Timestamp(System.currentTimeMillis());
                int year = rand.nextInt(21) + 2000; // Générer une année entre 2000 et 2020
                int month = rand.nextInt(12) + 1; // Générer un mois entre 1 et 12
                int day = rand.nextInt(28) + 1; // Générer un jour entre 1 et 28 (pour éviter les problèmes de février)
                java.sql.Date commercial_date = java.sql.Date.valueOf(LocalDate.of(year, month, day));
                String currency = "USD";
                MyBigDecimal amount = new MyBigDecimal(rand.nextDouble() * 1000);
                Integer transaction = rand.nextInt(100);
                Integer year_rank = (rand.nextBoolean()) ? rand.nextInt(10) : null;
                Boolean member = rand.nextBoolean();
                String transaction_type = (rand.nextBoolean()) ? "online" : "in-store";
                Integer new_customer = (rand.nextBoolean()) ? rand.nextInt(10) : null;
                Integer old_customer = (rand.nextBoolean()) ? rand.nextInt(10) : null;
                String acceptance = (rand.nextBoolean()) ? "accepted" : "rejected";
                ShopUserTransaction row = new ShopUserTransaction(shop_id, unique_id, card_token, customer_id, user_id, purchase_date, commercial_date, currency, amount, transaction, year_rank, member, transaction_type, new_customer, old_customer, acceptance);
                deltaLakeData.add(row);
            }
            Dataset<ShopUserTransaction> dataset = sparkSession.createDataset(deltaLakeData, Encoders.bean(ShopUserTransaction.class));
            dataset.withColumn("amount", col("amount").cast(DataTypes.createDecimalType(10, 2))).write().format("delta").mode("append").save(shopUserTransactionPath);
            //return dataset.collectAsList();
        } catch (Exception e) {
            log.error("Failed to write data to Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to write data to Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }

    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getAllShopUsersTransaction() {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath);
            Dataset<ShopUserTransaction> shopTransactions = df.as(Encoders.bean(ShopUserTransaction.class));
            return shopTransactions.collectAsList();
        } catch (Exception e) {
            log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUsersTransaction(Integer limit) {
        SparkSession sparkSession = null;
        try {
            Integer rowLimit = limit != null ? limit : 100;
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath).limit(rowLimit).cache();
            Dataset<ShopUserTransaction> shopTransactions = df.as(Encoders.bean(ShopUserTransaction.class));
            return shopTransactions.collectAsList();
        } catch (Exception e) {
            log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUsersTransaction(int limit, int pageNumber, int resultsPerPage) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath).limit(limit).cache();
            Dataset<ShopUserTransaction> shopTransactions = df.as(Encoders.bean(ShopUserTransaction.class));
            List<ShopUserTransaction> transactions = shopTransactions.collectAsList();
            int maxOffset = Math.max(0, transactions.size() - resultsPerPage);
            int offset = (pageNumber - 1) * resultsPerPage > maxOffset ? maxOffset : (pageNumber - 1) * resultsPerPage;
            return transactions.subList(offset, Math.min(transactions.size(), offset + resultsPerPage));
        } catch (Exception e) {
            log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUsersTransaction(int pageNumber, int resultsPerPage) {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Chargement des données à partir d'un fichier
            Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath);

            // Ajout d'un index aux données
            df = df.withColumn("index", org.apache.spark.sql.functions.monotonically_increasing_id());

            // Définition de l'index de départ et du nombre de lignes à récupérer
            int startFrom = (pageNumber - 1) * resultsPerPage;
            int limit = resultsPerPage;

            // Vérification de l'index de départ et du nombre de lignes à récupérer
            int startIndex = Math.min((int)df.count(), startFrom);

            // Sélection des données à partir de l'index de départ et du nombre de lignes à récupérer
            df = df.filter(col("index").geq(startIndex)).limit(limit);

            // Suppression de la colonne index
            df = df.drop("index");

            // Conversion du DataFrame en une liste d'objets ShopUserTransaction
            List<ShopUserTransaction> transactions = df.as(Encoders.bean(ShopUserTransaction.class)).collectAsList();
            return transactions;
        } catch (Exception e) {
                log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
                throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
            } finally {
                if (sparkSession != null) {
                    sparkSessionPool.returnSparkSession(sparkSession);
                }
            }

    }

   /*
   @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUsersTransaction(int pageSize, int pageNumber) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Row> df = sparkSession.read().format("delta").load(shopUserTransactionPath);
            Dataset<ShopUserTransaction> shopTransactions = df.as(Encoders.bean(ShopUserTransaction.class));

            // Define the window specification
            WindowSpec window = Window.orderBy("shop_id");

            // Define the start and end rows for the page
            int startRow = (pageNumber - 1) * pageSize + 1;
            int endRow = pageNumber * pageSize;

            // Add row number to the dataset
            Dataset<Row> shopTransactionsWithRowNum = shopTransactions.withColumn("row_num", org.apache.spark.sql.functions.row_number().over(window));

            // Filter the dataset to include only the rows for the current page
            Dataset<Row> pageData = shopTransactionsWithRowNum.filter(col("row_num").between(startRow, endRow));

            // Convert the dataset back to a list of ShopUserTransaction objects
            List<ShopUserTransaction> transactions = pageData.drop("row_num").dropDuplicates().as(Encoders.bean(ShopUserTransaction.class)).collectAsList();

            return transactions;
        } catch (Exception e) {
            log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }*/

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionByShopId(Long shopId) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<ShopUserTransaction> shopTransactions = sparkSession.read().format("delta").load(shopUserTransactionPath).filter(col("shop_id").equalTo(shopId)).as(Encoders.bean(ShopUserTransaction.class));
            return shopTransactions.collectAsList();
        } catch (Exception e) {
            log.error("Failed to load data from Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to load data from Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public ShopUserTransaction createShopUserTransaction(ShopUserTransactionDTO shopUserTransactionDTO) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> deltaDF = sparkSession.read().format("delta").load(shopUserTransactionPath);
            Object maxIdObj = deltaDF.agg(functions.max("shop_id")).head().get(0);
            Long maxId = maxIdObj != null ? ((Long) maxIdObj) : 0;
            Long newId = ++maxId;
            ShopUserTransaction shopUserTransaction = ModelMapperUtils.convertClass(shopUserTransactionDTO, ShopUserTransaction.class);
            shopUserTransaction.setShop_id(newId);
            Dataset<ShopUserTransaction> shopUserTransactionDf = sparkSession.createDataset(Collections.singletonList(shopUserTransaction), Encoders.bean(ShopUserTransaction.class));
            shopUserTransactionDf.withColumn("amount", col("amount").cast(DataTypes.createDecimalType(10, 2))).write().format("delta").mode("append").save(shopUserTransactionPath);
            return shopUserTransaction;
        } catch (Exception e) {
            log.error("Failed to write data to Delta table 'shop_user_transaction'", e);
            throw new RuntimeException("Failed to write data to Delta table 'shop_user_transaction'", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public ShopUserTransaction updateShopUserTransactionByShopId(Long shopId, ShopUserTransactionDTO shopUserTransactionDTO) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaTable deltaTable = DeltaTable.forPath(sparkSession, shopUserTransactionPath);
            ShopUserTransaction shopUserTransaction = ModelMapperUtils.convertClass(shopUserTransactionDTO, ShopUserTransaction.class);
            shopUserTransaction.setShop_id(shopId);
            Dataset<Row> shopUserTransactionDF = sparkSession.createDataFrame(Collections.singletonList(shopUserTransaction), ShopUserTransaction.class);
            deltaTable.alias("shopUserTransaction")
                    .merge(shopUserTransactionDF.alias("newData"), "shopUserTransaction.shop_id = newData.shop_id")
                    .whenMatched()
                    .updateAll()
                    .whenNotMatched()
                    .insertAll()
                    .execute();
            return shopUserTransaction;
        } catch (Exception e) {
            log.error("Failed update Data on Delta table 'shop_user_transaction' with id = "+shopId, e);
            throw new RuntimeException("Failed update Data on Delta table 'shop_user_transaction' with id = "+shopId, e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }


    @Override
    @Transactional
    public void deleteShopUserTransactionByShopId(Long shopId) {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> shopUserTransactionsDF = sparkSession.read().format("delta").load(shopUserTransactionPath);
            shopUserTransactionsDF.createOrReplaceTempView("shopUserTransactions");
            sparkSession.sql("DELETE FROM shopUserTransactions WHERE Shop_id = " + shopId);
        } catch (Exception e) {
            log.error("Failed delete shopUserTransaction with id = "+shopId, e);
            throw new RuntimeException("Failed delete shopUserTransaction with Shop_id = "+shopId, e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public void deleteAllShopUserTransaction() {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> shopUserTransactionsDF = sparkSession.read().format("delta").load(shopUserTransactionPath);
            shopUserTransactionsDF.createOrReplaceTempView("shopUserTransactions");
            sparkSession.sql("DELETE FROM shopUserTransactions");
        } catch (Exception e) {
            log.error("Failed delete all shopUserTransactions", e);
            throw new RuntimeException("Failed delete all shopUserTransactions", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }

    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsByVersionBeforeOrAtTimestamp(String date) throws ParseException {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), shopUserTransactionPath);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionBeforeOrAtTimestamp(Timestamp);
            Dataset<ShopUserTransaction> df = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(shopUserTransactionPath).as(Encoders.bean(ShopUserTransaction.class));

            return df.collectAsList();
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Error parsing date: " + date, e);
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version before or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version before or at timestamp: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsByVersionBeforeOrAtTimestamp(String date, int limit, int pageNumber, int resultsPerPage) throws ParseException {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), shopUserTransactionPath);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionBeforeOrAtTimestamp(Timestamp);
            Dataset<ShopUserTransaction> shopTransactions = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(shopUserTransactionPath).limit(limit).as(Encoders.bean(ShopUserTransaction.class)).cache();
            List<ShopUserTransaction> transactions = shopTransactions.collectAsList();
            int maxOffset = Math.max(0, transactions.size() - resultsPerPage);
            int offset = (pageNumber - 1) * resultsPerPage > maxOffset ? maxOffset : (pageNumber - 1) * resultsPerPage;
            return transactions.subList(offset, Math.min(transactions.size(), offset + resultsPerPage));
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Error parsing date: " + date, e);
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version before or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version before or at timestamp: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(String date, int limit, int pageNumber, int resultsPerPage) throws ParseException {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), shopUserTransactionPath);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionAtOrAfterTimestamp(Timestamp);
            Dataset<ShopUserTransaction> shopTransactions = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(shopUserTransactionPath).limit(limit).as(Encoders.bean(ShopUserTransaction.class)).cache();
            List<ShopUserTransaction> transactions = shopTransactions.collectAsList();
            int maxOffset = Math.max(0, transactions.size() - resultsPerPage);
            int offset = (pageNumber - 1) * resultsPerPage > maxOffset ? maxOffset : (pageNumber - 1) * resultsPerPage;
            return transactions.subList(offset, Math.min(transactions.size(), offset + resultsPerPage));
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Error parsing date: " + date, e);
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version before or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version before or at timestamp: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(String date) throws ParseException {

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), shopUserTransactionPath);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionAtOrAfterTimestamp(Timestamp);
            Dataset<ShopUserTransaction> df = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(shopUserTransactionPath).as(Encoders.bean(ShopUserTransaction.class));

            return df.collectAsList();
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Error parsing date: " + date, e);
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version after or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version after or at timestamp: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(Integer version) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<ShopUserTransaction> df = sparkSession.read().format("delta").option("versionAsOf", version).load(shopUserTransactionPath).as(Encoders.bean(ShopUserTransaction.class));
            return df.collectAsList();
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(Integer version, int limit, int pageNumber, int resultsPerPage)
    {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<ShopUserTransaction> shopTransactions = sparkSession.read().format("delta").option("versionAsOf", version).load(shopUserTransactionPath).limit(limit).as(Encoders.bean(ShopUserTransaction.class)).cache();
            List<ShopUserTransaction> transactions = shopTransactions.collectAsList();
            int maxOffset = Math.max(0, transactions.size() - resultsPerPage);
            int offset = (pageNumber - 1) * resultsPerPage > maxOffset ? maxOffset : (pageNumber - 1) * resultsPerPage;
            return transactions.subList(offset, Math.min(transactions.size(), offset + resultsPerPage));
        } catch (Exception e) {
            log.error("Error getting shopUserTransactions by version: " + e.getMessage(), e);
            throw new RuntimeException("Error getting shopUserTransactions by version: ", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
}