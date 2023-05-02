package com.izycap.deltalake.service;

import com.izycap.deltalake.DTO.ShopUserTransactionDTO;
import com.izycap.deltalake.entities.ShopUserTransaction;

import java.text.ParseException;
import java.util.List;

public interface ShopUserTransactionService
{
    void insertData(Integer numberOfData);
    List<ShopUserTransaction> getAllShopUsersTransaction();
    List<ShopUserTransaction> getShopUsersTransaction(Integer limit);
    //List<ShopUserTransaction> getShopUsersTransaction(int pageSize, int pageNumber);
    List<ShopUserTransaction> getShopUsersTransaction(int limit, int pageNumber, int resultsPerPage);
    public List<ShopUserTransaction> getShopUsersTransaction(int pageNumber, int resultsPerPage);
    List<ShopUserTransaction> getShopUserTransactionByShopId(Long shopId);
    ShopUserTransaction createShopUserTransaction(ShopUserTransactionDTO shopUserTransactionDTO);
    ShopUserTransaction updateShopUserTransactionByShopId(Long shopId, ShopUserTransactionDTO shopUserTransactionDTO);
    void deleteShopUserTransactionByShopId(Long shopId);
    void deleteAllShopUserTransaction();
    List<ShopUserTransaction> getShopUserTransactionsByVersionBeforeOrAtTimestamp(String date) throws ParseException;
    List<ShopUserTransaction> getShopUserTransactionsByVersionBeforeOrAtTimestamp(String date, int limit, int pageNumber, int resultsPerPage) throws ParseException;
    List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(String date, int limit, int pageNumber, int resultsPerPage) throws ParseException;
    List<ShopUserTransaction> getShopUserTransactionsByVersionAfterOrAtTimestamp(String date) throws ParseException;
    List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(Integer version);
    List<ShopUserTransaction> getShopUserTransactionsBySnapshotVersion(Integer version, int limit, int pageNumber, int resultsPerPage);
}
