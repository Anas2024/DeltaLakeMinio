package com.izycap.deltalake.controller;

import com.izycap.deltalake.DTO.ShopUserTransactionDTO;
import com.izycap.deltalake.entities.ShopUserTransaction;
import com.izycap.deltalake.service.ShopUserTransactionService;
import org.springframework.beans.factory.annotation.Autowired;
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
    public List<ShopUserTransaction> insertData() {
        return shopUserTransactionService.insertData();

    }
    @GetMapping("/shopUserTransactions")
    public List<ShopUserTransaction> getAllShopUserTransactions() {

        return shopUserTransactionService.getAllShopUsersTransaction();
    }
    @GetMapping("/shopUserTransactions/{shopId}")
    public List<ShopUserTransaction> getShopUserTransactionsByShopId(@RequestParam Long shopId) {
        return shopUserTransactionService.getShopUserTransactionByShopId(shopId);
    }
    @GetMapping("/shopUserTransactions/snapshot/before/{date}")
    public List<ShopUserTransaction> getEtudiantsByVersionBeforeOrAtTimestamp(String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionBeforeOrAtTimestamp(date);
    }
    @GetMapping("/shopUserTransactions/snapshot/after/{date}")
    public List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(String date) throws ParseException {
        return shopUserTransactionService.getShopUserTransactionsByVersionAfterOrAtTimestamp(date);
    }
    @GetMapping("/shopUserTransactions/snapshot/version/{version}")
    public List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(@RequestParam int version)
    {
        return shopUserTransactionService.getShopUserTransactionsBySnapshotVersion(version);
    }

    @PostMapping("/shopUserTransactions")
    public void createShopUserTransaction(@RequestBody ShopUserTransactionDTO shopUserTransactionDTO ) {
        shopUserTransactionService.createShopUserTransaction(shopUserTransactionDTO);
    }

    @PutMapping("/shopUserTransactions/{id}")
    public ShopUserTransaction updateShopUserTransactionByShopId(@RequestParam Long shopId, @RequestBody ShopUserTransactionDTO shopUserTransactionDTO)
    {
        return shopUserTransactionService.updateShopUserTransactionByShopId(shopId, shopUserTransactionDTO);
    }
    @DeleteMapping("/shopUserTransactions/{id}")
    void deleteShopUserTransactionByShopId(Long shopId)
    {
        shopUserTransactionService.deleteShopUserTransactionByShopId(shopId);
    }
    @DeleteMapping("/shopUserTransactions")
    void deleteShopUserTransactions()
    {
        shopUserTransactionService.deleteAllShopUserTransaction();
    }
}
