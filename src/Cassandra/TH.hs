{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Cassandra.TH
       ( makeSchema
       , fromEither
       , Text
       , Int32
       , Identity
       , UUID
       , Decimal
       , UTCTime
       , module Cassandra
       , module Data.Proxy
       ) where

import Data.Generics
import Language.Haskell.TH
import Language.Haskell.TH.Syntax hiding (lift)
import Language.Haskell.TH.Quote

import Data.Text (Text)
import qualified Data.Text as Text

import Control.Applicative
import Debug.Trace
import Data.Char
import Data.List.Split
import Data.List
import Data.Maybe
import Data.Int
import Data.UUID
import Data.Functor.Identity
import Data.Proxy
import Data.Decimal
import Data.Time.Clock

import Cassandra.Schema
import Cassandra.SchemaPrinter
import Cassandra.RecordTH
import Cassandra

import Database.CQL.Protocol hiding (Table (..), getTable, table, Map, keyspace)

import Control.Monad.State
import Data.Map.Lazy (Map)
import qualified Data.Map.Lazy as M
import Control.Lens
import Control.Lens.Getter

type TypeMap t = Map String t

data THState = THState
  { _udtTypes :: Map String Type
  , _udtExps :: Map String Exp
  , _udtCols :: Map String ColumnType
  , _typeMaps :: Map String Type
  , _typeSyns :: Map String String
  , _pkTypes :: Map String Type
  , _keyspace :: String
  , _schema :: Schema
  }

makeLenses ''THState

type SM = MonadState THState

type SchemaMonad = StateT THState Q

fromEither :: Either a b -> b
fromEither (Right x) = x
fromEither _ = error "fromEither found left"

schemaError :: SM m => String -> m a
schemaError msg = do
  ksp <- use keyspace
  fail $ "Error in schema for keyspace '" ++ ksp ++ "': " ++ msg

typemapError :: SM m => String -> m a
typemapError s = schemaError $
  "Couldn't find user defined type or synonym '" ++ s ++
  "'. Perhaps you tried to use it before you defined it?"

(!) :: SM m => TypeMap t -> String -> m t
udts ! s = case M.lookup s udts of
  Just t -> return t
  Nothing -> typemapError s

getMap :: (SM m, Ord k) => Lens' THState (Map k a) -> (k -> m a) -> k -> m a
getMap l mkDefault k = do
  m <- use l
  def <- mkDefault k
  let (res, m') = M.insertLookupWithKey (\_ _ -> id) k def m
  l .= m'
  return $ case res of
    Just x -> x
    Nothing -> def

makeSchema :: (Maybe FilePath) -> String -> Schema -> Q [Dec]
makeSchema mfp keyspace schema = do
  let printed = printSchema schema
      header = "USE " ++ keyspace ++ ";\n\n"
      output = header ++ printed

  case mfp of
    Just fp -> runIO (writeFile fp output)
    _ -> return ()
  evalStateT makeSchema'
    (THState mempty mempty mempty mempty mempty mempty keyspace schema)

makeSchema' :: SchemaMonad [Dec]
makeSchema' = do
  theSchema <- use schema

  makeTypeSyns theSchema
  makeTypeMaps theSchema

  theRecords <- makeSchemaRecords theSchema
  cqlInstances <- mapM udtCqlInstance (schemaUDTs theSchema)
  tableQIs <- mapM tableQueryableInstance (schemaTables theSchema)
  viewQIs <- mapM viewQueryableInstance (schemaViews theSchema)
  tablePKIs <- mapM tablePartitionKeyInstance (schemaTables theSchema)
  viewPKIs <- mapM viewPartitionKeyInstance (schemaViews theSchema)
  recordInstances <- lift $ mapM recordInstanceFromDec theRecords

  return $ theRecords ++
           concat recordInstances ++
           cqlInstances ++
           tablePKIs ++
           viewPKIs ++
           tableQIs ++
           viewQIs

applyFirst :: (a -> a) -> [a] -> [a]
applyFirst f (x : xs) = f x : xs
applyFirst _ [] = []

snakeToUpperCamel :: String -> String
snakeToUpperCamel = concat
             . map (applyFirst toUpper)
             . splitOn "_"

mkRecordName :: String -> Name
mkRecordName = mkName . snakeToUpperCamel

mkPkName :: String -> Name
mkPkName name = mkName $ snakeToUpperCamel name ++ "PK"

getTable :: SM m => String -> m (Maybe Table)
getTable name = do
  tables <- schemaTables <$> use schema
  return $ find ((== name) . tableName) tables

getPkType :: SM m => String -> m Type
getPkType = getMap pkTypes mkPkType
  where mkPkType name = do
          udts <- use udtTypes
          mtable <- getTable name
          (Table _ pk fields) <- case mtable of
            Just t -> return t
            Nothing -> schemaError
              "Attempt to make a primary key from an undefined table."

          let pkNames = pkToList pk
              pkTypes = mapMaybe (`lookup` fields) pkNames
          realTypes <- mapM getHaskellType pkTypes
          return $ case realTypes of
            [x] -> AppT (ConT $ mkName "Identity") x
            xs -> foldl AppT (TupleT $ length xs) xs

viewGetFields :: SM m => View -> m [Field]
viewGetFields (View name tableName fields) = do
  mtable <- getTable tableName
  (Table _ tablePk tableFields) <- case mtable of
    Just t -> return t
    _ -> schemaError $
           "Attempt to define view '" ++
           name ++
           "' on undefined table '" ++
           tableName ++ "'"

  unless (all (`elem` fields) (pkToList tablePk)) $
    schemaError $
      "Attempt to define view '" ++
      name ++
      "' on table '" ++
      tableName ++
      "' but missing some primary keys."

  unless (all (`elem` (map fst tableFields)) fields) $
    schemaError $
      "Attempt to define view '" ++
      name ++
      "' on table '" ++
      tableName ++
      "' but attempting to use some fields not in the table."

  let typeAField s =
        let (Just t) = lookup s tableFields
        in (s, t)

  return (map typeAField fields)

getRowName :: Name -> String -> Name
getRowName recordName name =
  mkName $ applyFirst toLower (nameBase recordName) ++
           "_" ++ applyFirst toLower (snakeToUpperCamel name)

getRealType :: SM m => CType -> m CType
getRealType (CList t) = CList <$> getRealType t
getRealType (CType t) = do
  syns <- use typeSyns
  case M.lookup t syns of
    Just x -> getRealType (CType x)
    Nothing -> return $ CType t

cassandraTypeToType :: SM m => CType -> m Type
cassandraTypeToType t = do
  let ret = return . ConT . mkName
  case t of
    CType s -> case s of
      "int" -> ret "Int32"
      "float" -> ret "Float"
      "double" -> ret "Double"
      "decimal" -> ret "Decimal"
      "bigint" -> ret "Integer"
      "text" -> ret "Text"
      "boolean" -> ret "Bool"
      "timestamp" -> ret "UTCTime"
      "uuid" -> ret "UUID"
      name -> do
        (CType realType) <- getRealType t
        if realType /= name
          then cassandraTypeToType (CType realType)
          else (! name) =<< use udtTypes

    CList t -> AppT ListT <$> cassandraTypeToType t

getHaskellType :: SM m => CType -> m Type
getHaskellType t = do
  typemap <- use typeMaps
  case M.lookup (ctypeToCql t) typemap of
    Just x -> return x
    Nothing -> cassandraTypeToType t

cassandraTypeToColumnType :: SM m => CType -> m ColumnType
cassandraTypeToColumnType t = do
  realType <- getRealType t
  case realType of
    CType s -> case s of
      "int" -> pure IntColumn
      "float" -> pure FloatColumn
      "double" -> pure DoubleColumn
      "decimal" -> pure DecimalColumn
      "bigint" -> pure BigIntColumn
      "text" -> pure TextColumn
      "boolean" -> pure BooleanColumn
      "timestamp" -> pure TimestampColumn
      "uuid" -> pure UuidColumn
      name -> (! name) =<< use udtCols
    CList t -> ListColumn <$> cassandraTypeToColumnType t

cassandraTypeToColumnTypeE :: SM m => CType -> m Exp
cassandraTypeToColumnTypeE t = do
  realType <- getRealType t
  let ret = return . ConE . mkName
  case realType of
    CType s -> case s of
      "int" -> ret "IntColumn"
      "float" -> ret "FloatColumn"
      "double" -> ret "DoubleColumn"
      "decimal" -> ret "DecimalColumn"
      "bigint" -> ret "BigIntColumn"
      "text" -> ret "TextColumn"
      "boolean" -> ret "BooleanColumn"
      "timestamp" -> ret "TimestampColumn"
      "uuid" -> ret "UuidColumn"
      name -> getUdtColumnTypeE name
    CList t -> AppE <$> (ret "ListColumn") <*> cassandraTypeToColumnTypeE t

------------------------------------------------------------------
-- type instance PartitionKey
------------------------------------------------------------------

unfoldApps :: Type -> [Type]
unfoldApps (AppT xs t) = unfoldApps xs ++ [t]
unfoldApps t = [t]

tablePartitionKeyInstance :: SM m => Table -> m Dec
tablePartitionKeyInstance t@(Table name pk _) =
  tablePartitionKeyInstanceWithName name t

viewPartitionKeyInstance :: SM m => View -> m Dec
viewPartitionKeyInstance (View name tableName _) = do
  mtable <- getTable tableName
  table <- case mtable of
    Just t -> return t
    _ -> schemaError $
           "Attempt to define view '" ++
           name ++
           "' on undefined table '" ++
           tableName ++ "'"

  tablePartitionKeyInstanceWithName name table

tablePartitionKeyInstanceWithName :: SM m => String -> Table -> m Dec
tablePartitionKeyInstanceWithName name t@(Table tname pk _) = do
  let rname = mkRecordName name
  pkType <- getPkType tname
  let n = length (pkGetPartitionKeys pk)
      partitionKeyType = case pkType of
        tuple@(AppT _ _) ->
          let parts = unfoldApps tuple
          in case parts of
            (TupleT _ : _ : _) ->
              if n > 1
              then foldl AppT (TupleT n) (take n $ tail parts)
              else AppT (ConT $ mkName "Identity") (head $ tail parts)
            (_ : [t]) -> AppT (ConT $ mkName "Identity") t
            _ -> TupleT 0
        i -> i

      inst = TySynEqn [ConT rname] partitionKeyType

  return $ TySynInstD (mkName "PartitionKey") inst

------------------------------------------------------------------
-- instance Queryable
------------------------------------------------------------------

getTupleType :: (SM m, HasFields t) => t -> m Type
getTupleType t = do
  let (_, fieldTypes) = unzip $ getFields t
  fieldTypesH <- mapM getHaskellType fieldTypes
  return $ foldl AppT (TupleT (length fieldTypesH)) fieldTypesH

tableQueryableInstance :: SM m => Table -> m Dec
tableQueryableInstance t@(Table name pk fields) = do
  pkType <- getPkType name
  tupleType <- getTupleType t
  mkQueryableInstance (mkRecordName name) name pk fields pkType tupleType

viewQueryableInstance :: forall m. SM m => View -> m Dec
viewQueryableInstance view@(View name tableName _) = do
  pkType <- getPkType tableName
  fields <- viewGetFields view
  tupleType <- getTupleType (View2 name fields)
  mtable <- getTable tableName
  table <- case mtable of
    Just t -> return t
    _ -> schemaError $
           "Attempt to define view '" ++
           name ++
           "' on undefined table '" ++
           tableName ++ "'"

  mkQueryableInstance (mkRecordName name) tableName
    (tablePk table) fields pkType tupleType

mkQueryableInstance
  :: SM m
     => Name -- ^ Type name
     -> String -- ^ Table name
     -> PrimaryKey -- ^ Primary key
     -> [Field] -- ^ Fields
     -> Type -- ^ Primary key type
     -> Type -- ^ Tuple type
     -> m Dec
mkQueryableInstance typeName tableName pk fields pkType tupleType = do
  ksp <- use keyspace
  udts <- use udtTypes
  return $
    InstanceD []
      (ConT (mkName "Queryable") `AppT`
       pkType `AppT`
       tupleType `AppT`
       ConT typeName)
      [ mkProxyTextFn (mkName "queryKeyspace") ksp
      , mkProxyTextFn (mkName "queryTable") tableName
      , mkQueryColumnNames fields
      , mkQueryPartitionKeyNames pk
      , mkQueryClusterKeyNames pk
      ]

mkProxyTextFn :: Name -> String -> Dec
mkProxyTextFn name toReturn =
  let body = LitE (StringL toReturn)
      clause = Clause [WildP] (NormalB body) []
  in FunD name [clause]

mkQueryColumnNames :: [Field] -> Dec
mkQueryColumnNames fields =
  let (fnames, _) = unzip fields
  in mkProxyListFn (mkName "queryColumnNames") fnames

mkQueryPartitionKeyNames :: PrimaryKey -> Dec
mkQueryPartitionKeyNames pk =
  mkProxyListFn (mkName "queryPartitionKeyNames") (pkGetPartitionKeys pk)

mkQueryClusterKeyNames :: PrimaryKey -> Dec
mkQueryClusterKeyNames pk =
  mkProxyListFn (mkName "queryClusterKeyNames") (pkGetClusterKeys pk)

mkProxyListFn :: Name -> [String] -> Dec
mkProxyListFn name toReturn =
  let body = ListE (map (LitE . StringL) toReturn)
      clause = Clause [WildP] (NormalB body) []
  in FunD name [clause]

------------------------------------------------------------------
-- instance Cql
------------------------------------------------------------------

udtCqlInstance :: SM m => UDT -> m Dec
udtCqlInstance t@(UDT name _) = do
  ctypeFn <- udtCtypeFn t
  return $ InstanceD [] (AppT (ConT $ mkName "Cql") (ConT $ mkRecordName name))
            [ ctypeFn
            , udtToCqlFn t
            , udtFromCqlFn t ]

getUdtColumnTypeE :: SM m => String -> m Exp
getUdtColumnTypeE = getMap udtExps mkUdtColumnTypeE
  where mkUdtColumnTypeE name = do
          ksp <- use keyspace
          theSchema <- use schema
          (UDT _ fields) <-
            case find ((== name) . getName) (schemaUDTs theSchema) of
              Just x -> return x
              Nothing -> schemaError $ "Type not found: '" ++ name ++ "'"

          let mapField (fname, ftype) = do
                colType <- cassandraTypeToColumnTypeE ftype
                return $ TupE [ LitE (StringL fname), colType ]

          fieldExps <- mapM mapField fields

          return $ foldl AppE (ConE $ mkName "UdtColumn")
                     [ AppE (ConE $ mkName "Keyspace") (LitE (StringL ksp))
                     , LitE (StringL name)
                     , ListE fieldExps ]

udtCtypeFn :: SM m => UDT -> m Dec
udtCtypeFn t@(UDT name fields) = do
  obj <- AppE (ConE $ mkName "Tagged") <$> getUdtColumnTypeE name
  return $ ValD (VarP (mkName "ctype")) (NormalB obj) []

udtToCqlClause :: UDT -> Clause
udtToCqlClause t@(UDT name fields) =
  Clause [pat] body []
  where (fnames, ftypes) = unzip fields
        fnamesT = map Text.pack fnames
        pat = ConP (mkRecordName name) pats
        patNames = map (\x -> 'a' : show x) [1 .. length fields]
        pats = map (VarP . mkName) patNames
        body = NormalB (AppE (ConE $ mkName "CqlUdt") $ ListE listItems)
        converted = map (AppE (VarE (mkName "toCql")) . VarE . mkName) patNames
        nameLits = map (LitE . StringL) fnames
        listItems = zipWith (\x y -> TupE [x, y]) nameLits converted

udtToCqlFn :: UDT -> Dec
udtToCqlFn t = FunD (mkName "toCql") [udtToCqlClause t]

udtFromCqlClauseMain :: UDT -> Clause
udtFromCqlClauseMain t@(UDT name fields) =
  Clause [pat] body []
  where pat = ConP (mkName "CqlUdt") [ListP lpats]
        patNames = map (\x -> 'a' : show x) [1 .. length fields]
        mkLPat s = TupP [WildP, VarP (mkName s)]
        lpats = map mkLPat patNames
        result = foldl AppE (ConE (mkRecordName name)) vars
        body = NormalB (AppE (ConE (mkName "Right")) result)
        vars = map (AppE (VarE (mkName "fromEither")) .
                    AppE (VarE (mkName "fromCql")) .
                    VarE . mkName) patNames

failClause :: String -> Clause
failClause failWith =
  Clause [WildP]
         (NormalB (AppE (ConE (mkName "Left"))
                        (LitE (StringL failWith))))
         []

udtFromCqlFn :: UDT -> Dec
udtFromCqlFn t@(UDT name _) =
  FunD (mkName "fromCql") [udtFromCqlClauseMain t, failClause failWith]
  where failWith = "Expected " ++ name

------------------------------------------------------------------------
-- data
------------------------------------------------------------------------

makeTypeMaps :: SM m => Schema -> m ()
makeTypeMaps schem = do
  let tm = schemaTypeMaps schem
      f (cql, hask) = (cql, ConT $ mkName hask)
      theMap = M.fromList (map f tm)

  typeMaps .= theMap

makeTypeSyns :: SM m => Schema -> m ()
makeTypeSyns schem = do
  let ts = schemaTypeSyns schem

  typeSyns .= M.fromList ts

makeSchemaRecords :: SM m => Schema -> m [Dec]
makeSchemaRecords schem = do
  typeRecords <- mapM makeUDTRecord (schemaUDTs schem)
  tableRecords <- mapM makeTableRecord (schemaTables schem)
  viewRecords <- mapM makeViewRecord (schemaViews schem)
  return $ typeRecords ++ tableRecords ++ viewRecords

makeUDTRecord :: SM m => UDT -> m Dec
makeUDTRecord t@(UDT name fields) = do
  udtTypes %= M.insert name (ConT $ mkRecordName name)
  makeRecord t

makeTableRecord :: SM m => Table -> m Dec
makeTableRecord = makeRecord

data View2 = View2 String [Field]

instance HasName View2 where
  getName (View2 name _) = name
instance HasFields View2 where
  getFields (View2 _ fields) = fields

makeViewRecord :: SM m => View -> m Dec
makeViewRecord view@(View name _ _) = do
  fields <- viewGetFields view
  makeRecord (View2 name fields)

makeRecord :: (HasName t, HasFields t, SM m) => t -> m Dec
makeRecord t = do
  udts <- use udtTypes
  let recordName = mkRecordName (getName t)
  rows <- mapM (makeRecordRow recordName) (getFields t)
  return $ DataD [] recordName [] [RecC recordName rows] [mkName "Show"]

makeRecordRow
  :: SM m
     => Name
     -> (String, CType)
     -> m VarStrictType

makeRecordRow recordName (name, ctype) = do
  udts <- use udtTypes
  let rowName = getRowName recordName name
  rowType <- getHaskellType ctype
  return (rowName, IsStrict, rowType)
