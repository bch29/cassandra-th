module Cassandra.SchemaPrinter (printSchema) where

import qualified Data.Map as M
import Data.Map (Map)
import Data.List

import Cassandra.Schema

data SimpleSchema = SimpleSchema
  { simpleUDTs   :: [UDT]
  , simpleTables :: [Table]
  }

simplifySchema :: Schema -> SimpleSchema
simplifySchema (Schema udts tables typeSyns _ _) =
  let tsm = M.fromList typeSyns
      reduceType (CType t) = case M.lookup t tsm of
        Just x -> reduceType (CType x)
        Nothing -> CType t
      reduceType (CList t) = CList $ reduceType t

      reduceFieldTypes fs =
        let (fnames, ftypes) = unzip fs
        in zip fnames (map reduceType ftypes)

      reduceUdtTypes (UDT name fields) =
        UDT name (reduceFieldTypes fields)

      reduceTableTypes (Table name pk fields) =
        Table name pk (reduceFieldTypes fields)

  in SimpleSchema (map reduceUdtTypes udts) (map reduceTableTypes tables)

printCType :: CType -> String
printCType (CType t) = t
printCType (CList t) = "list<" ++ printCType t ++ ">"

printField :: (String, CType) -> String
printField (fname, ftype) = "  " ++ fname ++ " " ++ printCType ftype

printFields :: [(String, CType)] -> String
printFields = intercalate ",\n" . map printField

printUDT :: UDT -> String
printUDT (UDT name fields) =
  "CREATE TYPE " ++ name ++ " (\n" ++ printFields fields ++ "\n);\n\n"

printPk :: PrimaryKey -> String
printPk (PKSingle t ts) =
  "  PRIMARY KEY (" ++ intercalate ", " (t : ts) ++ ")"
printPk (PKTupled xs ys) =
  "  PRIMARY KEY ((" ++ intercalate ", " xs ++
  "), " ++ intercalate ", " ys ++ ")"

printTable :: Table -> String
printTable (Table name pk fields) =
  "CREATE TABLE " ++ name ++ " (\n" ++
  printFields fields ++ ",\n" ++ printPk pk ++ "\n);\n\n"

printSimpleSchema :: SimpleSchema -> String
printSimpleSchema (SimpleSchema udts tables) = (printUDT =<< udts) ++ (printTable =<< tables)

printSchema :: Schema -> String
printSchema = printSimpleSchema . simplifySchema
