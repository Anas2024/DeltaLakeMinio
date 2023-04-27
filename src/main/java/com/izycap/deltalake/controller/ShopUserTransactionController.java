package com.izycap.deltalake.controller;

import com.izycap.deltalake.DTO.ShopUserTransactionDTO;
import com.izycap.deltalake.entities.ShopUserTransaction;
import com.izycap.deltalake.service.ShopUserTransactionService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.List;

@RestController
@RequestMapping("api/v1")
public class ShopUserTransactionController
{
    @Autowired
    private ShopUserTransactionService shopUserTransactionService;

    @PostMapping("/insertData")
    @Operation(summary = "Insert random data")
    @Order(1)
    public void insertData(@RequestParam(defaultValue = "100", required = false) Integer numberOfRow) {
        shopUserTransactionService.insertData(numberOfRow);

    }
    @Operation(summary = "Retrieves Shop User Transactions")
    @Order(2)
    @GetMapping("/shopUserTransactions")
    public List<ShopUserTransaction> getAllShopUserTransactions() {

        return shopUserTransactionService.getAllShopUsersTransaction();
    }
    @Operation(summary = "Retrieves Shop User Transactions with a row limit to return")
    @Order(3)
    @GetMapping("/shopUserTransactions/limit/{limit}")
    public List<ShopUserTransaction> getShopUserTransactions(@RequestParam(defaultValue = "100", required = true) Integer limit) {

        return shopUserTransactionService.getShopUsersTransaction(limit);
    }
    @Operation(summary = "Retrieves Shop User Transactions by shopId ")
    @Order(4)
    @GetMapping("/shopUserTransactions/shopId/{shopId}")
    public List<ShopUserTransaction> getShopUserTransactionsByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId) {
        return shopUserTransactionService.getShopUserTransactionByShopId(shopId);
    }
    @Operation(summary = "Retrieves snapshot of Shop User Transactions Before or at specific timestamp")
    @Order(5)
    @GetMapping("/shopUserTransactions/snapshot/before/{date}")
    public List<ShopUserTransaction> getEtudiantsByVersionBeforeOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionBeforeOrAtTimestamp(date);
    }
    @Operation(summary = "Retrieves snapshot of Shop User Transactions after or at specific timestamp")
    @Order(6)
    @GetMapping("/shopUserTransactions/snapshot/after/{date}")
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionAfterOrAtTimestamp(date);
    }
    @Operation(summary = "Retrieves snapshot of Shop User Transactions by version")
    @Order(7)
    @GetMapping("/shopUserTransactions/snapshot/version/{version}")
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(@RequestParam(defaultValue = "0", required = true) int version)
    {
        return shopUserTransactionService.getShopUserTransactionsBySnapshotVersion(version);
    }
    @Operation(summary = "Retrieves snapshot of Shop User Transactions by version")
    @Order(8)
    @PostMapping("/shopUserTransactions")
    public void createShopUserTransaction(@RequestBody ShopUserTransactionDTO shopUserTransactionDTO ) {
        shopUserTransactionService.createShopUserTransaction(shopUserTransactionDTO);
    }
    @Operation(summary = "Update shop user transaction by shopId")
    @Order(9)
    @PutMapping("/shopUserTransactions/{shopId}")
    public ShopUserTransaction updateShopUserTransactionByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId, @RequestBody ShopUserTransactionDTO shopUserTransactionDTO)
    {
        return shopUserTransactionService.updateShopUserTransactionByShopId(shopId, shopUserTransactionDTO);
    }
    @Operation(summary = "Delete shop user transaction by shopId")
    @Order(10)
    @DeleteMapping("/shopUserTransactions/{shopId}")
    void deleteShopUserTransactionByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId)
    {
        shopUserTransactionService.deleteShopUserTransactionByShopId(shopId);
    }
    @Operation(summary = "Delete all shop user transaction")
    @Order(11)
    @DeleteMapping("/shopUserTransactions")
    void deleteShopUserTransactions()
    {
        shopUserTransactionService.deleteAllShopUserTransaction();
    }
}
