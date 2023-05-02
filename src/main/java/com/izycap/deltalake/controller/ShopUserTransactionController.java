package com.izycap.deltalake.controller;

import com.izycap.deltalake.DTO.ShopUserTransactionDTO;
import com.izycap.deltalake.entities.ShopUserTransaction;
import com.izycap.deltalake.service.ShopUserTransactionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.List;

@RestController
@RequestMapping("api/v1")
public class ShopUserTransactionController
{
    @Autowired
    @Qualifier("shopUserTransactionImpl")
    private ShopUserTransactionService shopUserTransactionService;

    @PostMapping("/insertData")
    @Operation(summary = "Insert random data")
    @Tag(name = "1. POST")
    public void insertData(@RequestParam(defaultValue = "100", required = false) Integer numberOfRow) {
        shopUserTransactionService.insertData(numberOfRow);

    }
    @PostMapping("/shopUserTransactions")
    @Operation(summary = "Create Shop user transaction")
    @Tag(name = "1. POST")
    public void createShopUserTransaction(@RequestBody ShopUserTransactionDTO shopUserTransactionDTO ) {
        shopUserTransactionService.createShopUserTransaction(shopUserTransactionDTO);
    }

    @GetMapping("/shopUserTransactions")
    @Operation(summary = "Retrieves Shop User Transactions")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getAllShopUserTransactions() {

        return shopUserTransactionService.getAllShopUsersTransaction();
    }

    @GetMapping("/shopUserTransactions/limit/{limit}")
    @Operation(summary = "Retrieves Shop User Transactions with a row limit to return")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getShopUserTransactions(@RequestParam(defaultValue = "100", required = true) Integer limit) {

        return shopUserTransactionService.getShopUsersTransaction(limit);
    }
    /*@GetMapping("/shopUserTransactions/pageSize/{pageSize}/pageNumber/{pageNumber}")
    @Operation(summary = "Retrieves Shop User Transactions with a row limit to return")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getShopUserTransactions(@RequestParam(defaultValue = "100", required = true) Integer pageSize, @RequestParam(defaultValue = "1", required = true) Integer pageNumber) {

        return shopUserTransactionService.getShopUsersTransaction(pageSize, pageNumber);
    }*/

    @GetMapping("/shopUserTransactions/limit/{limit}/pageNumber/{pageNumber}/resultsPerPage/{resultsPerPage}")
    @Operation(summary = "Retrieves Shop User Transactions with a row limit to return")
    @Tag(name = "3. GET Per Page and Row Limit")
    public List<ShopUserTransaction> getShopUserTransactions(@RequestParam(defaultValue = "100", required = true) Integer limit,
                                                             @RequestParam(defaultValue = "1", required = true) Integer pageNumber,
                                                             @RequestParam(defaultValue = "50", required = true) Integer resultsPerPage) {

        return shopUserTransactionService.getShopUsersTransaction(limit, pageNumber, resultsPerPage);
    }
    @GetMapping("/shopUserTransactions/pageNumber/{pageNumber}/resultsPerPage/{resultsPerPage}")
    @Operation(summary = "Retrieves Shop User Transactions with a row limit to return")
    @Tag(name = "3. GET Per Page and Row Limit")
    public List<ShopUserTransaction> getShopUserTransactions(@RequestParam(defaultValue = "1", required = true) Integer pageNumber,
                                                             @RequestParam(defaultValue = "50", required = true) Integer resultsPerPage) {

        return shopUserTransactionService.getShopUsersTransaction(pageNumber, resultsPerPage);
    }

    @GetMapping("/shopUserTransactions/shopId/{shopId}")
    @Operation(summary = "Retrieves Shop User Transactions by shopId")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getShopUserTransactionsByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId) {
        return shopUserTransactionService.getShopUserTransactionByShopId(shopId);
    }

    @GetMapping("/shopUserTransactions/snapshot/before/{date}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions Before or at specific timestamp")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getEtudiantsByVersionBeforeOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionBeforeOrAtTimestamp(date);
    }


    @GetMapping("/shopUserTransactions/snapshot/before/{date}/limit/{limit}/pageNumber/{pageNumber}/resultsPerPage/{resultsPerPage}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions before or at specific timestamp")
    @Tag(name = "3. GET Per Page and Row Limit")
    public List<ShopUserTransaction> getEtudiantsByVersionBeforeOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date,
                                                                              @RequestParam(defaultValue = "100", required = true) Integer limit,
                                                                              @RequestParam(defaultValue = "1", required = true) Integer pageNumber,
                                                                              @RequestParam(defaultValue = "50", required = true) Integer resultsPerPage) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionBeforeOrAtTimestamp(date, limit, pageNumber, resultsPerPage);
    }

    @GetMapping("/shopUserTransactions/snapshot/after/{date}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions after or at specific timestamp")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionAfterOrAtTimestamp(date);
    }
    @GetMapping("/shopUserTransactions/snapshot/after/{date}/limit/{limit}/pageNumber/{pageNumber}/resultsPerPage/{resultsPerPage}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions after or at specific timestamp")
    @Tag(name = "3. GET Per Page and Row Limit")
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(@RequestParam(defaultValue = "2023-04-26", required = true) String date,
                                                                                        @RequestParam(defaultValue = "100", required = true) Integer limit,
                                                                                        @RequestParam(defaultValue = "1", required = true) Integer pageNumber,
                                                                                        @RequestParam(defaultValue = "50", required = true) Integer resultsPerPage) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionAfterOrAtTimestamp(date, limit, pageNumber, resultsPerPage);
    }

    @GetMapping("/shopUserTransactions/snapshot/version/{version}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions by version")
    @Tag(name = "2. GET")
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(@RequestParam(defaultValue = "0", required = true) int version)
    {
        return shopUserTransactionService.getShopUserTransactionsBySnapshotVersion(version);
    }

    @GetMapping("/shopUserTransactions/snapshot/version/{version}/limit/{limit}/pageNumber/{pageNumber}/resultsPerPage/{resultsPerPage}")
    @Operation(summary = "Retrieves snapshot of Shop User Transactions by version")
    @Tag(name = "3. GET Per Page and Row Limit")
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(@RequestParam(defaultValue = "0", required = true) int version,
                                                                              @RequestParam(defaultValue = "100", required = true) Integer limit,
                                                                              @RequestParam(defaultValue = "1", required = true) Integer pageNumber,
                                                                              @RequestParam(defaultValue = "50", required = true) Integer resultsPerPage)
    {
        return shopUserTransactionService.getShopUserTransactionsBySnapshotVersion(version, limit, pageNumber, resultsPerPage);
    }

    @PutMapping("/shopUserTransactions/{shopId}")
    @Operation(summary = "Update shop user transaction by shopId")
    @Tag(name = "4. PUT")
    public ShopUserTransaction updateShopUserTransactionByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId, @RequestBody ShopUserTransactionDTO shopUserTransactionDTO)
    {
        return shopUserTransactionService.updateShopUserTransactionByShopId(shopId, shopUserTransactionDTO);
    }

    @DeleteMapping("/shopUserTransactions/{shopId}")
    @Operation(summary = "Delete shop user transaction by shopId")
    @Tag(name = "5. DELETE")
    void deleteShopUserTransactionByShopId(@RequestParam(defaultValue = "1", required = true) Long shopId)
    {
        shopUserTransactionService.deleteShopUserTransactionByShopId(shopId);
    }

    @DeleteMapping("/shopUserTransactions")
    @Operation(summary = "Delete all shop user transaction")
    @Tag(name = "5. DELETE")
    void deleteShopUserTransactions()
    {
        shopUserTransactionService.deleteAllShopUserTransaction();
    }
}
