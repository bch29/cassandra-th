module Cassandra.QQ (schema) where

import Data.Generics
import Language.Haskell.TH.Quote
import Language.Haskell.TH
import Cassandra.Schema

schema :: QuasiQuoter
schema = QuasiQuoter { quoteExp = quoteSchemaExp
                     , quotePat = undefined
                     , quoteType = undefined
                     , quoteDec = undefined
                     }

quoteSchemaExp s = do
  loc <- location
  let pos = ( loc_filename loc
            , fst (loc_start loc)
            , snd (loc_start loc))
  theSchema <- parseSchema pos s
  case checkSchema theSchema of
    Right _ -> dataToExpQ (const Nothing) theSchema
    Left msg -> fail msg
