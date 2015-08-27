{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

module Cassandra
       ( Queryable (..)
       , PartitionKey
       , WhereClause , unWhereClause , mkWhereClause
       , selectByPrimaryKey'
       , selectCustom'
       , selectByPartitionKey'
       , cassInsert'
       , cassDelete'
       , cassDeleteByPartitionKey'
       ) where

import Data.Proxy
import Database.CQL.Protocol
import Database.CQL.IO
import Data.Int

import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as Text

import Data.Monoid

type family PartitionKey table

class (Tuple pk, Record a, t ~ TupleType a, Tuple t)
      => Queryable pk t a | a -> pk , a -> t where

  queryKeyspace    :: Proxy a -> Text
  queryTable       :: Proxy a -> Text
  queryColumnNames :: Proxy a -> [Text]
  queryPartitionKeyNames :: Proxy a -> [Text]
  queryClusterKeyNames :: Proxy a -> [Text]

queryPrimaryKeyNames :: (Queryable pk t a) => Proxy a -> [Text]
queryPrimaryKeyNames proxy =
  queryPartitionKeyNames proxy ++ queryClusterKeyNames proxy

formatWhere :: [Text] -> Text
formatWhere = Text.intercalate " and " . map (<> " = ?")

formatSelectors :: [Text] -> Text
formatSelectors = Text.intercalate ", "

-- | A CQL 'WHERE' clause.
newtype WhereClause = WhereClause Text

-- | Get the text representation of a @WhereClause@.
unWhereClause :: WhereClause -> Text
unWhereClause (WhereClause s) = s

-- | Make a @WhereClause@, ensuring that it is properly formatted.
mkWhereClause :: Text -> Maybe WhereClause
mkWhereClause = Just . WhereClause

-- | Generates a CQL query that selects a row from a table
-- using the given primary key.
querySelectByPrimaryKey
  :: Queryable pk t a => Proxy a -> QueryString R pk t
querySelectByPrimaryKey proxy =
  QueryString $
       "SELECT "
    <> formatSelectors (queryColumnNames proxy)
    <> " FROM "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> " WHERE "
    <> formatWhere (queryPrimaryKeyNames proxy)

querySelectByPartitionKey
  :: Queryable pk t a => Proxy a -> QueryString R (PartitionKey a) t
querySelectByPartitionKey proxy =
  QueryString $
       "SELECT "
    <> formatSelectors (queryColumnNames proxy)
    <> " FROM "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> " WHERE "
    <> formatWhere (queryPartitionKeyNames proxy)

querySelectCustom
  :: Queryable pk t a => Proxy a -> [WhereClause] -> QueryString R b t
querySelectCustom proxy whereClauses =
  QueryString $
       "SELECT "
    <> formatSelectors (queryColumnNames proxy)
    <> " FROM "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> (case whereClauses of
          [] -> mempty
          _ ->
            " WHERE "
            <> (Text.intercalate " and " . map unWhereClause) whereClauses)

queryInsert
  :: Queryable pk t a => Proxy a -> QueryString W t ()
queryInsert proxy =
  QueryString $
       "INSERT INTO "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> " ("
    <> Text.intercalate ", " (queryColumnNames proxy)
    <> ")"
    <> " VALUES "
    <> "("
    <> Text.intercalate ", "
         (map (const "?") [1 .. length (queryColumnNames proxy)])
    <> ")"

queryDelete
  :: Queryable pk t a => Proxy a -> QueryString W pk ()
queryDelete proxy =
  QueryString $
       "DELETE FROM "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> " WHERE "
    <> formatWhere (queryPrimaryKeyNames proxy)

queryDeleteByPartitionKey
  :: (Queryable pk t a, Tuple (PartitionKey a))
     => Proxy a -> QueryString W (PartitionKey a) ()
queryDeleteByPartitionKey proxy =
  QueryString $
       "DELETE FROM "
    <> queryKeyspace proxy
    <> "."
    <> queryTable proxy
    <> " WHERE "
    <> formatWhere (queryPartitionKeyNames proxy)

selectByPrimaryKey
  :: (MonadClient m, Queryable pk t a)
     => Proxy a -> pk -> QueryParams pk -> m (Response R pk t)
selectByPrimaryKey proxy pk params =
  let params' = params { values = pk }
  in runQ (querySelectByPrimaryKey proxy) params'

selectByPrimaryKey'
  :: forall a t pk m. (MonadClient m, Queryable pk t a)
     => pk -> m [a]
selectByPrimaryKey' pk = do
  let params = QueryParams One True pk Nothing Nothing Nothing
  response <- runQ (querySelectByPrimaryKey (Proxy :: Proxy a)) params
  return $ case response of
    RsResult _ (RowsResult _ rows) -> map asRecord rows
    _ -> []

selectCustom'
  :: forall a t b pk m. (MonadClient m, Queryable pk t a, Tuple b)
     => [WhereClause] -> b -> m [a]
selectCustom' whereClauses vals = do
  let params = QueryParams One True vals Nothing Nothing Nothing
  response <- runQ (querySelectCustom (Proxy :: Proxy a) whereClauses) params
  return $ case response of
    RsResult _ (RowsResult _ rows) -> map asRecord rows
    _ -> []

selectByPartitionKey'
  :: forall a t pk m. (MonadClient m, Queryable pk t a, Tuple (PartitionKey a))
     => PartitionKey a -> m [a]
selectByPartitionKey' pk = do
  let params = QueryParams One True pk Nothing Nothing Nothing
  response <- runQ (querySelectByPartitionKey (Proxy :: Proxy a)) params
  return $ case response of
    RsResult _ (RowsResult _ rows) -> map asRecord rows
    _ -> []

cassInsert'
  :: forall a t pk m. (MonadClient m, Queryable pk t a, Show t)
     => [a] -> m ()
cassInsert' [] = return ()
cassInsert' rows = batch $ do
  setConsistency One
  let theQuery = queryInsert (Proxy :: Proxy a)
  mapM_ (addQuery theQuery . asTuple) rows

cassDelete'
  :: (MonadClient m, Queryable pk t a, Show t)
     => Proxy a -> pk -> m ()
cassDelete' proxy pk = do
  let params = QueryParams One True pk Nothing Nothing Nothing
  runQ (queryDelete proxy) params
  return ()

cassDeleteByPartitionKey'
  :: (MonadClient m, Queryable pk t a, Show t, Tuple (PartitionKey a))
     => Proxy a -> PartitionKey a -> m ()
cassDeleteByPartitionKey' proxy pk = do
  let params = QueryParams One True pk Nothing Nothing Nothing
  runQ (queryDeleteByPartitionKey proxy) params
  return ()
