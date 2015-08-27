{-# LANGUAGE DeriveDataTypeable #-}

module Cassandra.Schema
       ( CType (..)
       , PrimaryKey (..)
       , UDT (..)
       , Table (..)
       , View (..)
       , Field
       , Schema (..)
       , HasName (..)
       , HasFields (..)
       , parseSchema
       , checkSchema
       , pkGetPartitionKeys
       , pkGetClusterKeys
       , pkToList
       , ctypeToCql )
       where

import Data.Generics
import Text.ParserCombinators.Parsec
import qualified Text.Parsec.Token as P
import Control.Monad
import Data.Monoid
import Data.Either
import Data.List
import Data.Maybe

import Debug.Trace

data CType =
    CType String
  | CList CType
  deriving (Show, Data, Typeable)

data PrimaryKey = PKTupled [String] [String] | PKSingle String [String]
                deriving (Show, Data, Typeable)

pkGetPartitionKeys :: PrimaryKey -> [String]
pkGetPartitionKeys (PKSingle x _) = [x]
pkGetPartitionKeys (PKTupled xs _) = xs

pkGetClusterKeys :: PrimaryKey -> [String]
pkGetClusterKeys (PKSingle _ xs) = xs
pkGetClusterKeys (PKTupled _ xs) = xs

pkToList :: PrimaryKey -> [String]
pkToList pk = pkGetPartitionKeys pk ++ pkGetClusterKeys pk

type Field = (String, CType)

class HasFields t where
  getFields :: t -> [Field]

class HasName t where
  getName :: t -> String

data UDT =
  UDT String [Field]
  deriving (Show, Data, Typeable)

data Table = Table
  { tableName     :: String
  , tablePk       :: PrimaryKey
  , tableFields   :: [Field]
  } deriving (Show, Data, Typeable)

data View = View
  { viewName :: String
  , viewTable :: String
  , viewFields :: [String]
  } deriving (Show, Data, Typeable)

type TypeSyn = (String, String)
type TypeMap = (String, String)

data Schema = Schema
  { schemaUDTs  :: [UDT]
  , schemaTables :: [Table]
  , schemaTypeSyns :: [TypeSyn]
  , schemaTypeMaps :: [TypeMap]
  , schemaViews :: [View]
  } deriving (Show, Data, Typeable)

udtSchema     t = Schema [t] [] [] [] []
tableSchema   t = Schema [] [t] [] [] []
typesynSchema t = Schema [] [] [t] [] []
typemapSchema t = Schema [] [] [] [t] []
viewSchema    t = Schema [] [] [] [] [t]

instance Monoid Schema where
  mappend (Schema a0 a1 a2 a3 a4) (Schema b0 b1 b2 b3 b4) =
    Schema (a0 <> b0) (a1 <> b1) (a2 <> b2) (a3 <> b3) (a4 <> b4)
  mempty = Schema [] [] [] [] []

instance HasFields Table where
  getFields = tableFields

instance HasFields UDT where
  getFields (UDT _ fields) = fields

instance HasName Table where
  getName = tableName

instance HasName UDT where
  getName (UDT name _) = name

cqlDef = P.LanguageDef
  { P.commentStart = "/*"
  , P.commentEnd = "*/"
  , P.commentLine = "--"
  , P.nestedComments = False
  , P.identStart = letter <|> char '_'
  , P.identLetter = alphaNum <|> char '_'
  , P.opStart = char '+'
  , P.opLetter = char '+'
  , P.reservedNames = ["table", "udt", "end"]
  , P.reservedOpNames = []
  , P.caseSensitive = False
  }

lexer = P.makeTokenParser cqlDef

parens     = P.parens     lexer
braces     = P.braces     lexer
identifier = P.identifier lexer
reserved   = P.reserved   lexer
symbol     = P.reserved   lexer
angles     = P.angles     lexer
comma      = P.comma      lexer
commaSep   = P.commaSep   lexer
whitespace = P.whiteSpace lexer

schema :: CharParser st Schema
schema = do
  whitespace
  elems <- many schemaElement
  return $ mconcat elems

cassandraType :: CharParser st CType
cassandraType =
  (try (CList <$> (symbol "list" >> angles cassandraType))) <|>
  (CType <$> identifier)

ctypeToCql :: CType -> String
ctypeToCql (CType s) = s
ctypeToCql (CList t) = "list" ++ ('<' : ctypeToCql t ++ ">")

field :: CharParser st Field
field = do
  id <- identifier
  typ <- cassandraType
  return (id, typ)

partitionKey :: CharParser st (Either [String] String)
partitionKey =
  parens (Left <$> commaSep identifier) <|>
  Right <$> identifier

primaryKey :: CharParser st PrimaryKey
primaryKey = do
  partKey <- partitionKey
  rest <- option [] $ do
    comma
    commaSep identifier
  return $ case partKey of
    Left k -> PKTupled k rest
    Right k -> PKSingle k rest

table :: CharParser st Table
table = do
  reserved "table"
  tableName <- identifier
  pk <- parens primaryKey
  fields <- many1 field
  reserved "end"
  return $ Table tableName pk fields

aTable = tableSchema <$> table

udt :: CharParser st UDT
udt = do
  reserved "udt"
  name <- identifier
  fields <- many1 field
  reserved "end"
  return $ UDT name fields

aUdt = udtSchema <$> udt

typesyn :: CharParser st TypeSyn
typesyn = do
  reserved "typesyn"
  newName <- identifier
  otherName <- identifier
  return $ (newName, otherName)

aTypesyn = typesynSchema <$> typesyn

typemap :: CharParser st TypeMap
typemap = do
  reserved "typemap"
  cqlName <- identifier
  haskellName <- identifier
  return $ (cqlName, haskellName)

aTypemap = typemapSchema <$> typemap

view :: CharParser st View
view = do
  reserved "view"
  viewName <- identifier
  tableName <- parens identifier
  theFields <- many1 identifier
  reserved "end"
  return $ View viewName tableName theFields

aView = viewSchema <$> view

schemaElement :: CharParser st Schema
schemaElement = aTable <|> aUdt <|> aTypesyn <|> aTypemap <|> aView

testTable = unlines
  [ "typesyn user_id int"
  , "typemap user_id UserId"
  , ""
  , "table user (user_id)"
  , "  user_id uuid"
  , "  name text"
  , "end"
  , ""
  , "udt my_udt"
  , "  udt_field tt"
  , "end"
  , "table other ((other_id, asd), two)"
  , "  other_id uuid"
  , "  other_name text"
  , "end"
  , "view user_view (user) user_id end"
  ]

testInput = "(a, x), b, c"

testParser p s = runParser p () "" s

test = testParser schema testTable

parseSchema :: Monad m => (String, Int, Int) -> String -> m Schema
parseSchema (file, line, col) s =
    case runParser p () "" s of
      Left err  -> fail $ show err
      Right r   -> return r
  where
    p = do pos <- getPosition
           setPosition $
             flip setSourceName file $
             flip setSourceLine line $
             flip setSourceColumn col $
             pos
           result <- schema
           eof
           return result

checkSchema :: Schema -> Either String ()
checkSchema (Schema udts tables typeSyns typeMaps views) = return ()
  --   typeNames <- mapM checkType udts
  --   tableNames <- mapM (checkTable typeNames) tables
  --   checkNoDuplicateTables tableNames
  --   return ()
  -- where checkTable :: [String] -> Table -> Either String String
  --       checkTable typeNames (Table name pk fields) = do
  --         let fieldNames = map fst fields
  --         unless (nub fieldNames == fieldNames) $
  --           Left ("Repeated field name in table " ++ name)

  --         let allPkParts = case pk of
  --               PKSingle x xs -> x : xs
  --               PKTupled xs ys -> xs ++ ys

  --         unless (all (`elem` fieldNames) allPkParts) $
  --           Left ("Undefined field used as primary key in table " ++ name)

  --         let getUDT (_, CCustom t) = Just t
  --             getUDT _ = Nothing

  --             udtsUsed = mapMaybe getUDT fields

  --         let undefinedNames =
  --               filter (not . (`elem` typeNames)) udtsUsed
  --         unless (null undefinedNames) $
  --           Left ("Undefined udts in table "
  --                 ++ name ++ ": " ++ show undefinedNames)

  --         return name

  --       checkType (UDT name fields) = do
  --         let fieldNames = map fst fields
  --         unless (nub fieldNames == fieldNames) $
  --           Left ("Repeated field name in type " ++ name)

  --         return name

  --       checkNoDuplicateTables tableNames = do
  --         let duplicates = filter ((>1) . length) . group . sort $ tableNames
  --         unless (null duplicates) $
  --           Left ("Duplicate table names found: " ++ show duplicates)
